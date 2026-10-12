# Graph tracing

Graph tracing uses OpenTelemetry alongside the existing Prometheus metrics.
It is disabled by default. To enable OTLP export, set an endpoint or set
`OTEL_TRACES_EXPORTER=otlp` on the controller.

For a collector that accepts OTLP over HTTP:

```yaml
env:
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: http://otel-collector.observability.svc:4318
  - name: OTEL_EXPORTER_OTLP_PROTOCOL
    value: http/protobuf
  - name: OTEL_SERVICE_NAME
    value: kro
```

The default protocol is `grpc`. Use port 4317 for a typical gRPC collector.
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`
override their general counterparts. The exporters also support standard
OTEL settings for headers, TLS, compression, and timeouts. The SDK reads
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`,
and batch processor settings. Set `OTEL_TRACES_EXPORTER=none` or
`OTEL_SDK_DISABLED=true` to disable export. The default service name is `kro`.

The Graph controller must be enabled through the `GraphKind` feature gate.
Annotation propagation is a separate opt-in setting. Enable it with the
controller flag `--enable-trace-context-propagation=true`, or with Helm:

```yaml
config:
  enableTraceContextPropagation: true
```

It defaults to `false`. Enabling it lets Graph authors supply context to link to.
With propagation disabled, the controller still emits
Graph and node spans when export is configured, but ignores Graph trace
annotations and does not add context to resources. Existing resource context
is retained to avoid changing metadata when the setting is disabled.

With propagation enabled, associate a Graph with an existing trace by
providing W3C trace context to link to:

```yaml
metadata:
  annotations:
    kro.run/traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
```

The optional `kro.run/tracestate` annotation carries vendor trace state.
Invalid trace context is ignored; reconciliation continues normally.

Each reconciliation attempt starts a new trace with a `graph.reconcile` root
span. When propagation is enabled, valid annotated context becomes a span
link rather than a parent. Each attempt has its own trace ID and sampling
decision; linked sampling flags and vendor trace state are not inherited.
Each node produces a `graph.node` child span, including skipped or waiting nodes.
Nested graph nodes contain spans for their child nodes. Attributes identify
the Graph, its generation, the node path and kind, and the outcome. Waiting
is recorded as an event; failures set the span's error status.

With propagation enabled, template resources receive their node span's context
in `kro.run/traceparent` and, when present, `kro.run/tracestate`. Collection
members created in the same attempt share their node's context. The first
applied context is preserved on later reconciliations so
tracing does not cause metadata changes on every attempt. Recreated resources
receive fresh context. Referenced and patched resources are not annotated.
No tracing state is stored in Graph status. RGD and instance reconciliation
are outside the scope of this instrumentation.

## Kubernetes tracing proposals

[KEP-647: API Server Tracing](https://github.com/kubernetes/enhancements/tree/master/keps/sig-instrumentation/647-apiserver-tracing)
and [KEP-2831: Kubelet Tracing](https://github.com/kubernetes/enhancements/tree/master/keps/sig-instrumentation/2831-kubelet-tracing)
use OpenTelemetry and propagate context through HTTP and gRPC requests. They
do not define the resource annotation mechanism used here.

[KEP-5915: Async Trace Context Propagation](https://github.com/kubernetes/enhancements/pull/5916)
is an open, unmerged proposal for a `tracing.k8s.io/traceparent` annotation.
It proposes span links across asynchronous reconciliation boundaries.
This POC uses `StartReconcileSpan`, `ExtractContext`, and `InjectContext`
helper names like the proposal. Reconciliation spans use the consumer kind
and links across reconciliation boundaries, while keeping `kro.run` annotation
keys and the opt-in flag. Injection also preserves existing resource context
across retries to avoid metadata changes. It does not implement
the proposed component-base library or standardized annotation keys.

The [KEP review](https://github.com/kubernetes/enhancements/pull/5916#discussion_r2800850429)
raises conflicts when multiple field managers update one trace annotation.
Other comments discuss
[sampling linked traces](https://github.com/kubernetes/enhancements/pull/5916#discussion_r2800979862)
and [subresource permissions](https://github.com/kubernetes/enhancements/pull/5916#discussion_r2801118292).
These are useful points for upstream review. kro currently limits injection
to template resources, keeps their first applied context stable, and makes
annotation propagation an explicit opt-in. It does not annotate patch targets
or add annotation-only API writes.
