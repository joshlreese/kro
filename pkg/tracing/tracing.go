// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tracing instruments Graph reconciliation and propagates node context.
package tracing

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

const (
	// TraceParentAnnotation carries W3C trace context between controllers.
	TraceParentAnnotation = "kro.run/traceparent"
	// TraceStateAnnotation carries optional vendor trace state.
	TraceStateAnnotation = "kro.run/tracestate"
)

type graphTracingKey struct{}

type graphTracing struct {
	tracer               trace.Tracer
	propagateAnnotations bool
}

// StartGraphSpan starts a reconciliation span and optionally reads the parent annotation.
// A nil tracer uses the global provider configured at controller startup.
func StartGraphSpan(
	ctx context.Context, g *v1alpha1.Graph, tracer trace.Tracer, propagateAnnotations bool,
) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer("github.com/kubernetes-sigs/kro/graph")
	}
	if propagateAnnotations {
		carrier := propagation.MapCarrier{
			"traceparent": g.Annotations[TraceParentAnnotation],
			"tracestate":  g.Annotations[TraceStateAnnotation],
		}
		ctx = (propagation.TraceContext{}).Extract(ctx, carrier)
	}
	ctx = context.WithValue(ctx, graphTracingKey{}, graphTracing{
		tracer: tracer, propagateAnnotations: propagateAnnotations,
	})
	return tracer.Start(ctx, "graph.reconcile", trace.WithAttributes(
		attribute.String("k8s.namespace.name", g.Namespace),
		attribute.String("k8s.resource.name", g.Name),
		attribute.String("k8s.resource.uid", string(g.UID)),
		attribute.Int64("kro.graph.generation", g.Generation),
		attribute.Bool("kro.graph.deleting", !g.DeletionTimestamp.IsZero()),
	))
}

// StartNodeSpan starts a child span during a Graph reconciliation.
func StartNodeSpan(ctx context.Context, id, kind string) (context.Context, trace.Span) {
	config, ok := ctx.Value(graphTracingKey{}).(graphTracing)
	if !ok {
		return ctx, nil
	}
	parent := trace.SpanContextFromContext(ctx)
	ctx, span := config.tracer.Start(ctx, "graph.node", trace.WithAttributes(
		attribute.String("kro.node.id", id), attribute.String("kro.node.kind", kind),
	))
	sc := span.SpanContext()
	ctx = context.WithValue(ctx, nodeSpanKey{}, sc.IsValid() && sc.SpanID() != parent.SpanID())
	return ctx, span
}

// EndSpan records an outcome and ends the span. Waiting is an expected outcome.
func EndSpan(span trace.Span, err, waiting error, outcome string) {
	if span == nil {
		return
	}
	if err != nil {
		if waiting != nil && errors.Is(err, waiting) {
			outcome = "waiting"
			span.AddEvent("waiting", trace.WithAttributes(attribute.String("reason", err.Error())))
		} else {
			outcome = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}
	span.SetAttributes(attribute.String("kro.outcome", outcome))
	span.End()
}

// PropagateTraceContext adds node trace context to resource annotations when enabled.
// It preserves existing context across retries and when propagation is disabled
// to avoid metadata changes that would trigger another reconciliation.
func PropagateTraceContext(ctx context.Context, obj, current *unstructured.Unstructured) {
	config, ok := ctx.Value(graphTracingKey{}).(graphTracing)
	if !ok {
		return
	}
	carrier := propagation.MapCarrier{}
	if current != nil && current.GetAnnotations()[TraceParentAnnotation] != "" {
		carrier["traceparent"] = current.GetAnnotations()[TraceParentAnnotation]
		carrier["tracestate"] = current.GetAnnotations()[TraceStateAnnotation]
	} else {
		// A no-op provider can return the incoming parent without creating a span.
		// StartNodeSpan marks contexts for which a new node span was actually created.
		if !config.propagateAnnotations || !hasNodeSpan(ctx) {
			return
		}
		(propagation.TraceContext{}).Inject(ctx, carrier)
	}
	if carrier["traceparent"] == "" {
		return
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[TraceParentAnnotation] = carrier["traceparent"]
	if carrier["tracestate"] != "" {
		annotations[TraceStateAnnotation] = carrier["tracestate"]
	} else {
		delete(annotations, TraceStateAnnotation)
	}
	obj.SetAnnotations(annotations)
}

type nodeSpanKey struct{}

func hasNodeSpan(ctx context.Context) bool {
	created, _ := ctx.Value(nodeSpanKey{}).(bool)
	return created
}
