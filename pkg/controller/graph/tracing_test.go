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

package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
	"github.com/kubernetes-sigs/kro/pkg/tracing"
)

func TestReconcileTracing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ready", nil}, {"waiting", executor.ErrNotReady}, {"failure", errors.New("apply failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := graph("traced")
			const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
			g.Annotations = map[string]string{tracing.TraceParentAnnotation: parent}
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
			r := &Reconciler{Client: newClient(t, g), Compiler: &fakeCompiler{program: &compiler.Program{}}, Registry: registry.New(), Executor: &fakeExecutor{applyErr: tc.err}, Tracer: provider.Tracer("test"), EnableTraceContextPropagation: true}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(g)}
			for attempt := 0; attempt < 2; attempt++ {
				result, err := r.Reconcile(context.Background(), req)
				if tc.name == "failure" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				if tc.name == "waiting" {
					require.Positive(t, result.RequeueAfter)
				}
			}
			spans := recorder.Ended()
			require.Len(t, spans, 2)
			require.NotEqual(t, spans[0].SpanContext().SpanID(), spans[1].SpanContext().SpanID())
			require.NotEqual(t, spans[0].SpanContext().TraceID(), spans[1].SpanContext().TraceID())
			sc := trace.SpanContextFromContext((propagation.TraceContext{}).Extract(context.Background(), propagation.MapCarrier{"traceparent": parent}))
			for _, span := range spans {
				require.Equal(t, "graph.reconcile", span.Name())
				require.NotEqual(t, sc.TraceID(), span.SpanContext().TraceID())
				require.False(t, span.Parent().IsValid())
				require.Len(t, span.Links(), 1)
				require.Equal(t, sc, span.Links()[0].SpanContext)
				if tc.name == "failure" {
					require.Equal(t, codes.Error, span.Status().Code)
				} else {
					require.NotEqual(t, codes.Error, span.Status().Code)
				}
			}
		})
	}
}

func TestReconcileTracingPropagationDisabledByDefault(t *testing.T) {
	t.Parallel()
	g := graph("traced")
	g.Annotations = map[string]string{tracing.TraceParentAnnotation: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	r := &Reconciler{
		Client: newClient(t, g), Compiler: &fakeCompiler{program: &compiler.Program{}},
		Registry: registry.New(), Executor: &fakeExecutor{}, Tracer: provider.Tracer("test"),
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(g)})
	require.NoError(t, err)
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.False(t, spans[0].Parent().IsValid(), "the default must ignore annotated context")
}
