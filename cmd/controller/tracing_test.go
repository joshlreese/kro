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

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func clearTracingEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"OTEL_SDK_DISABLED", "OTEL_TRACES_EXPORTER", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
		"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
	} {
		t.Setenv(key, "")
	}
}

func TestTracingDisabledByDefault(t *testing.T) {
	clearTracingEnv(t)
	for _, tc := range []struct{ name, exporter, endpoint, disabled string }{
		{name: "default"},
		{name: "explicit none", exporter: "none", endpoint: "http://localhost:4318"},
		{name: "disabled SDK", exporter: "otlp", disabled: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", tc.exporter)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_SDK_DISABLED", tc.disabled)
			before := otel.GetTracerProvider()
			shutdown, err := setupTracing(context.Background())
			require.NoError(t, err)
			require.NoError(t, shutdown(context.Background()))
			require.Same(t, before, otel.GetTracerProvider())
		})
	}
}

func TestTracingRejectsUnsupportedConfiguration(t *testing.T) {
	clearTracingEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	_, err := setupTracing(context.Background())
	require.ErrorContains(t, err, "OTEL_TRACES_EXPORTER")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")
	_, err = setupTracing(context.Background())
	require.ErrorContains(t, err, "protocol")
}

func TestTracingHTTPExportAndShutdown(t *testing.T) {
	clearTracingEnv(t)
	received := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		request := &collectortrace.ExportTraceServiceRequest{}
		if proto.Unmarshal(body, request) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- request
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_SERVICE_NAME", "kro-test")
	original := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(original) })
	shutdown, err := setupTracing(context.Background())
	require.NoError(t, err)
	_, span := otel.Tracer("test").Start(context.Background(), "exported")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, shutdown(ctx))
	select {
	case request := <-received:
		require.Len(t, request.ResourceSpans, 1)
		resource := request.ResourceSpans[0]
		found := false
		for _, attr := range resource.Resource.Attributes {
			if attr.Key == "service.name" {
				require.Equal(t, "kro-test", attr.Value.GetStringValue())
				found = true
			}
		}
		require.True(t, found)
		require.Equal(t, "exported", resource.ScopeSpans[0].Spans[0].Name)
	case <-ctx.Done():
		t.Fatal("no OTLP request received")
	}
}
