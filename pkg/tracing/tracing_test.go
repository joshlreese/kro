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

package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

const testParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func TestParentExtraction(t *testing.T) {
	t.Parallel()
	for _, value := range []string{testParent, "invalid", ""} {
		t.Run(value, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
			g := &v1alpha1.Graph{}
			g.Annotations = map[string]string{TraceParentAnnotation: value}
			_, span := StartGraphSpan(context.Background(), g, provider.Tracer("test"), true)
			span.End()
			ended := recorder.Ended()[0]
			if value == testParent {
				parent := trace.SpanContextFromContext((propagation.TraceContext{}).Extract(context.Background(), propagation.MapCarrier{"traceparent": value}))
				require.Equal(t, parent.TraceID(), ended.SpanContext().TraceID())
				require.Equal(t, parent.SpanID(), ended.Parent().SpanID())
				require.True(t, ended.Parent().IsRemote())
			} else {
				require.False(t, ended.Parent().IsValid())
			}
		})
	}
}

func TestPropagationWithoutRecording(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		provider       trace.TracerProvider
		wantAnnotation bool
	}{
		{"disabled", noop.NewTracerProvider(), false},
		{"unsampled", sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample())), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &v1alpha1.Graph{}
			g.Annotations = map[string]string{TraceParentAnnotation: testParent}
			ctx, graphSpan := StartGraphSpan(context.Background(), g, tc.provider.Tracer("test"), true)
			defer graphSpan.End()
			ctx, nodeSpan := StartNodeSpan(ctx, "node", "template")
			defer nodeSpan.End()
			obj := &unstructured.Unstructured{}
			PropagateTraceContext(ctx, obj, nil)
			value := obj.GetAnnotations()[TraceParentAnnotation]
			if tc.wantAnnotation {
				require.NotEmpty(t, value)
				sc := trace.SpanContextFromContext((propagation.TraceContext{}).Extract(context.Background(), propagation.MapCarrier{"traceparent": value}))
				require.Equal(t, nodeSpan.SpanContext().SpanID(), sc.SpanID())
				require.False(t, sc.IsSampled())
			} else {
				require.Empty(t, value)
			}
		})
	}
}

func TestAnnotationPropagationOptIn(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
			g := &v1alpha1.Graph{}
			g.Annotations = map[string]string{TraceParentAnnotation: testParent, TraceStateAnnotation: "vendor=value"}
			ctx, graphSpan := StartGraphSpan(context.Background(), g, provider.Tracer("test"), enabled)
			nodeCtx, nodeSpan := StartNodeSpan(ctx, "node", "template")
			obj := &unstructured.Unstructured{}
			PropagateTraceContext(nodeCtx, obj, nil)
			nodeSpan.End()
			graphSpan.End()
			spans := recorder.Ended()
			require.Len(t, spans, 2, "propagation must not control span recording")
			require.Equal(t, graphSpan.SpanContext().SpanID(), spans[0].Parent().SpanID())
			if enabled {
				require.True(t, spans[1].Parent().IsRemote())
				require.NotEmpty(t, obj.GetAnnotations()[TraceParentAnnotation])
				require.Equal(t, "vendor=value", obj.GetAnnotations()[TraceStateAnnotation])
			} else {
				require.False(t, spans[1].Parent().IsValid())
				require.Empty(t, obj.GetAnnotations())
			}
		})
	}
}
