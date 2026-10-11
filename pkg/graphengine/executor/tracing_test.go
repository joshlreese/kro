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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/tracing"
)

func TestGraphTracingPropagation(t *testing.T) {
	t.Parallel()
	g := collectionCMGraph()
	g.Spec.Nodes = append(g.Spec.Nodes, scalarCMGraph("scalar").Spec.Nodes[0])
	g.Spec.Nodes[2].ID = "scalar"
	child := generator.NewGraph("child", generator.WithDef("value", map[string]any{"x": "v"}),
		generator.WithTemplate("nested", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "nested"},
			"data": map[string]any{"x": "${value.x}"},
		}))
	generator.WithSubgraph("sub", child)(g)
	generator.WithRef("ref", &v1alpha1.ExternalRef{APIVersion: "v1", Kind: "ConfigMap", Metadata: v1alpha1.ExternalRefMetadata{Name: "external"}})(g)
	generator.WithPatch("patch", "v1", "ConfigMap", "external", map[string]any{"data": map[string]any{"extra": "v"}})(g)
	g.Annotations = map[string]string{
		tracing.TraceParentAnnotation: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		tracing.TraceStateAnnotation:  "vendor=value",
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(liveCM("external")).Build()
	ex := NewSimple(cl)
	contexts := map[string]string{}
	for attempt := 0; attempt < 2; attempt++ {
		ctx, graphSpan := tracing.StartGraphSpan(context.Background(), g, provider.Tracer("test"), true)
		_, err := ex.Apply(ctx, compileAndBuild(t, g), watchrouter.NoopWatcher{})
		require.NoError(t, err)
		graphSpan.End()
		ended := recorder.Ended()
		graphSC := graphSpan.SpanContext()
		nodes := map[string]sdktrace.ReadOnlySpan{}
		for _, span := range ended {
			if span.Name() == "graph.node" && span.SpanContext().TraceID() == graphSC.TraceID() {
				nodes[spanAttribute(span, "kro.node.id")] = span
			}
		}
		require.Len(t, nodes, 8)
		require.Equal(t, graphSC.SpanID(), nodes["cm"].Parent().SpanID())
		require.Equal(t, nodes["sub"].SpanContext().SpanID(), nodes["sub/nested"].Parent().SpanID())
		for name, node := range map[string]string{"cm-alpha": "cm", "cm-beta": "cm", "scalar": "scalar", "nested": "sub/nested"} {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(configMapGVK)
			require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, obj))
			value := obj.GetAnnotations()[tracing.TraceParentAnnotation]
			require.NotEmpty(t, value)
			require.Equal(t, "vendor=value", obj.GetAnnotations()[tracing.TraceStateAnnotation])
			if attempt == 0 {
				contexts[name] = value
				extracted := (propagation.TraceContext{}).Extract(ctx, propagation.MapCarrier{"traceparent": value})
				require.Equal(t, nodes[node].SpanContext().SpanID(), trace.SpanContextFromContext(extracted).SpanID())
			} else {
				require.Equal(t, contexts[name], value, "retries must preserve resource context")
			}
		}
		external := &unstructured.Unstructured{}
		external.SetGroupVersionKind(configMapGVK)
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: "default", Name: "external"}, external))
		require.Empty(t, external.GetAnnotations()[tracing.TraceParentAnnotation])
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(configMapGVK)
	key := client.ObjectKey{Namespace: "default", Name: "cm-alpha"}
	require.NoError(t, cl.Get(context.Background(), key, obj))
	require.NoError(t, cl.Delete(context.Background(), obj))
	ctx, graphSpan := tracing.StartGraphSpan(context.Background(), g, provider.Tracer("test"), true)
	_, err := ex.Apply(ctx, compileAndBuild(t, g), watchrouter.NoopWatcher{})
	graphSpan.End()
	require.NoError(t, err)
	require.NoError(t, cl.Get(ctx, key, obj))
	require.NotEqual(t, contexts["cm-alpha"], obj.GetAnnotations()[tracing.TraceParentAnnotation])

}

func spanAttribute(span sdktrace.ReadOnlySpan, key attribute.Key) string {
	for _, attr := range span.Attributes() {
		if attr.Key == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

func TestGraphTracingNodeOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, outcome string
		modify        func(*v1alpha1.Graph)
		fail          bool
	}{
		{name: "ready", outcome: "ready"},
		{name: "skipped", outcome: "skipped", modify: func(g *v1alpha1.Graph) { g.Spec.Nodes[0].IncludeWhen = []string{"${false}"} }},
		{name: "waiting", outcome: "waiting", modify: func(g *v1alpha1.Graph) { g.Spec.Nodes[0].ReadyWhen = []string{"${false}"} }},
		{name: "failure", outcome: "error", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := scalarCMGraph("cm")
			if tc.modify != nil {
				tc.modify(g)
			}
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
			ctx, graphSpan := tracing.StartGraphSpan(context.Background(), g, provider.Tracer("test"), true)
			defer graphSpan.End()
			var cl client.Client = fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
			if tc.fail {
				cl = &patchFailClient{Client: cl, err: errors.New("apply failed")}
			}
			_, err := NewSimple(cl).Apply(ctx, compileAndBuild(t, g), watchrouter.NoopWatcher{})
			if tc.outcome == "waiting" {
				require.ErrorIs(t, err, ErrNotReady)
			} else if tc.fail {
				require.ErrorContains(t, err, "apply failed")
			} else {
				require.NoError(t, err)
			}
			spans := recorder.Ended()
			require.Len(t, spans, 1)
			require.Equal(t, tc.outcome, spanAttribute(spans[0], "kro.outcome"))
			require.Equal(t, graphSpan.SpanContext().SpanID(), spans[0].Parent().SpanID())
			if tc.fail {
				require.Equal(t, codes.Error, spans[0].Status().Code)
			} else {
				require.NotEqual(t, codes.Error, spans[0].Status().Code)
			}
			if tc.outcome == "waiting" {
				require.Equal(t, "waiting", spans[0].Events()[0].Name)
			}
		})
	}
}

func TestGraphTracingDoesNotInstrumentInstanceWalk(t *testing.T) {
	t.Parallel()
	g := scalarCMGraph("cm")
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	_, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})
	require.NoError(t, err)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(configMapGVK)
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "cm"}, obj))
	require.Empty(t, obj.GetAnnotations()[tracing.TraceParentAnnotation])
}

func TestGraphTracingAnnotationPropagationDisabled(t *testing.T) {
	t.Parallel()
	g := collectionCMGraph()
	const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	g.Annotations = map[string]string{tracing.TraceParentAnnotation: parent}
	existing := liveCM("cm-alpha")
	existing.SetAnnotations(map[string]string{
		tracing.TraceParentAnnotation: parent,
		tracing.TraceStateAnnotation:  "vendor=value",
	})
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(existing).Build()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	ctx, graphSpan := tracing.StartGraphSpan(context.Background(), g, provider.Tracer("test"), false)
	_, err := NewSimple(cl).Apply(ctx, compileAndBuild(t, g), watchrouter.NoopWatcher{})
	graphSpan.End()
	require.NoError(t, err)
	require.Len(t, recorder.Ended(), 3)
	for _, name := range []string{"cm-alpha", "cm-beta"} {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(configMapGVK)
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, obj))
		if name == "cm-alpha" {
			require.Equal(t, parent, obj.GetAnnotations()[tracing.TraceParentAnnotation])
			require.Equal(t, "vendor=value", obj.GetAnnotations()[tracing.TraceStateAnnotation])
		} else {
			require.Empty(t, obj.GetAnnotations()[tracing.TraceParentAnnotation])
			require.Empty(t, obj.GetAnnotations()[tracing.TraceStateAnnotation])
		}
	}
}
