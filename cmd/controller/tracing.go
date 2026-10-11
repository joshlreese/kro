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
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// setupTracing enables OTLP export when an endpoint or OTEL_TRACES_EXPORTER is set.
// The SDK and exporters read their standard OTEL_* environment variables.
func setupTracing(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	exporterName := os.Getenv("OTEL_TRACES_EXPORTER")
	if os.Getenv("OTEL_SDK_DISABLED") == "true" || exporterName == "none" {
		return noop, nil
	}
	if exporterName == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return noop, nil
	}
	if exporterName != "" && exporterName != "otlp" {
		return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER %q: use otlp or none", exporterName)
	}
	traceResource, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "kro")),
		resource.WithFromEnv(), resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("configure trace resource: %w", err)
	}
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	var spanExporter sdktrace.SpanExporter
	switch protocol {
	case "", "grpc":
		spanExporter, err = otlptracegrpc.New(ctx)
	case "http/protobuf":
		spanExporter, err = otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP trace protocol %q: use grpc or http/protobuf", protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(traceResource), sdktrace.WithBatcher(spanExporter))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
