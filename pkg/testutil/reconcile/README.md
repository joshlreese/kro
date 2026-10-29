# Test one apply pass

`reconcile.Run` compiles a Graph, or an RGD and instance, and runs the graph
executor once against a fresh controller-runtime fake client. It does not
need a cluster, kubeconfig, Docker, or another controller.

Use the library in Go tests:

```go
result, err := reconcile.Run(ctx, reconcile.Input{
    Graph: graph,
    Observations: []reconcile.Observation{
        {
            Node: "deployment",
            Object: &unstructured.Unstructured{Object: map[string]any{
                "status": map[string]any{"availableReplicas": int64(3)},
            }},
        },
    },
})
require.NoError(t, err)
require.True(t, result.Ready, result.Message)
require.NoError(t, reconcile.Check(result, reconcile.Expected{
    Resources: expectedManifests,
}))
```

Import `github.com/kubernetes-sigs/kro/pkg/testutil/reconcile`. The library
has no dependency on the CLI or `testing.T`. Inputs are copied, so a caller
can reuse them across cases. Each call starts with fresh state.

The same inputs and assertions work in the CLI. From `cmd/kro`, run:

```sh
go run . test -f ../../examples/testing/observed-deployment.yaml
```

The command writes the result as YAML and returns an error if compilation,
execution, or an assertion fails. A waiting result is not an execution
error. Assert `ready: true` when the case should finish ready.

## Inputs

Supply `graph`, or `rgd` and `instance`. `resources` contains manifests that
exist from the start, including any observed status. Use it for external
references and patch targets. Supply custom API definitions through `crds`.
The RGD instance CRD is derived using KRO's existing builder.

To test a project manifest directly, use `graphFile` or `rgdFile` instead of
embedding the definition. Paths are relative to the test case file, so tests
work regardless of the directory you run the command from. Absolute paths
also work. Each referenced file must contain one Graph or RGD manifest.

```yaml
graphFile: ../graphs/application.yaml
observations:
- node: deployment
  object:
    status:
      availableReplicas: 3
expect:
  ready: true
```

For an RGD, keep the test instance alongside the reference:

```yaml
rgdFile: ../rgds/application.yaml
instance:
  apiVersion: kro.run/v1alpha1
  kind: Application
  metadata:
    name: example
    namespace: default
  spec:
    replicas: 3
expect:
  ready: true
```

Provide exactly one of `graph`, `graphFile`, `rgd`, or `rgdFile`. Inline inputs
remain supported. The Go library can load the same files with
`tc, err := reconcile.LoadCase("tests/application.yaml")`, then pass `tc.Input`
to `reconcile.Run`. `Run` itself takes objects and does not read files.

`observations` associates existing state with template nodes. An observation
appears when its template's identity resolves, before the executor reads the
resource. Fields from the observation describe existing state; the executor
then applies the desired template. It may retain or replace those fields
according to apply ownership. Observations do not change between nodes or
simulate later controller activity.

Omitted `apiVersion`, `kind`, name, and namespace come from the resolved
template. Supplied identity fields must match it. An observation cannot
replace a resource already supplied in `resources`.

Use `node: backend/database` for a node inside a nested graph. For collections,
`index: 0` selects the first item, `index: 1` the second, and so on. A fixture
is introduced only once, including when patch nodes later modify that resource.
Fixtures for skipped or unresolved templates remain unused and are reported in
`unusedObservations`. Fixtures for refs, defs, and patch nodes are not supported;
use `resources` for existing objects those nodes read or patch.

## Outputs and assertions

`writes` records attempted apply, patch, and status update operations, including
the submitted object and any write error. `resources` contains the resulting
objects, including initial resources and the RGD instance. `ready`, `waiting`,
`message`, and `unresolved` describe the executor's outcome. Results are returned
alongside execution errors so Go tests can inspect partial progress.

`expect.resources` checks the fields you supply and allows extra object fields.
Each expected resource needs `apiVersion`, `kind`, and `metadata.name`; include
its namespace for namespaced objects. Arrays must match in length and order.
`expect.ready` and `expect.waiting` are optional boolean checks. Go callers can
also assert directly on writes and resulting objects.

## Scope

This tests the graph compiler, runtime, and executor. It does not run the whole
controller reconcile: inventory persistence, pruning, finalizers, controller
conditions, impersonation, and watch delivery are outside this utility.

Built-in schemas and REST mappings come from client-go's embedded OpenAPI test
snapshots, which cover core v1, apps v1, batch v1, discovery v1, and networking
v1alpha1. These snapshots do not represent every API supported by a current
cluster. A kind without an offline schema fails compilation. Custom API schemas
and scopes come from the supplied CRDs. The fake client uses its built-in apply
type converters and deduced types for custom APIs; custom list ownership may
differ from an API server.

The fake client does not run API admission, validation, defaulting, RBAC, or
third-party controllers. Supply the observed fields your graph needs and keep
integration tests for behavior that depends on a real API server or controller.
