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

package reconcile_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/testutil/reconcile"
)

func object(t *testing.T, data string) *unstructured.Unstructured {
	t.Helper()
	var obj unstructured.Unstructured
	require.NoError(t, yaml.Unmarshal([]byte(data), &obj.Object))
	return &obj
}

func TestObservedStateReachesDependentInOnePass(t *testing.T) {
	g := generator.NewGraph("test", generator.WithNamespace("default"),
		generator.WithDef("settings", map[string]any{"name": "web"}),
		generator.WithTemplate("deployment", map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "${settings.name}"}, "spec": map[string]any{"replicas": int64(3)}}),
		generator.WithReadyWhen("${deployment.status.availableReplicas == 3}"),
		generator.WithTemplate("output", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "result"}, "data": map[string]any{"replicas": "${string(deployment.status.availableReplicas)}"}}),
	)
	fixture := object(t, `status: {availableReplicas: 3}`)
	before := g.DeepCopy()
	result, err := reconcile.Run(context.Background(), reconcile.Input{Graph: g, Observations: []reconcile.Observation{{Node: "deployment", Object: fixture}}})
	require.NoError(t, err)
	require.True(t, result.Ready, result.Message)
	require.Len(t, result.Writes, 2)
	require.Equal(t, before, g)
	require.Equal(t, "", fixture.GetName())
	require.Empty(t, result.UnusedObservations)
	want := object(t, `apiVersion: v1
kind: ConfigMap
metadata: {name: result, namespace: default}
data: {replicas: "3"}`)
	require.NoError(t, reconcile.Check(result, reconcile.Expected{Resources: []*unstructured.Unstructured{want}}))
	result, err = reconcile.Run(context.Background(), reconcile.Input{Graph: g})
	require.NoError(t, err)
	require.True(t, result.Waiting)
	require.False(t, result.Ready)
	require.Len(t, result.Writes, 1)
}

func TestCollectionAndNestedFixtures(t *testing.T) {
	child := generator.NewGraph("child", generator.WithTemplate("configs", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "${configName}"}, "data": map[string]any{"desired": "yes"}}, v1alpha1.ForEachDimension{"configName": "${[\"a\", \"b\"]}"}))
	g := generator.NewGraph("test", generator.WithNamespace("default"), generator.WithSubgraph("child", child))
	fixture := object(t, `data: {observed: preserved}`)
	result, err := reconcile.Run(context.Background(), reconcile.Input{Graph: g, Observations: []reconcile.Observation{{Node: "child/configs", Index: 1, Object: fixture}}})
	require.NoError(t, err)
	require.True(t, result.Ready, result.Message)
	require.Len(t, result.Writes, 2)
	require.Empty(t, result.UnusedObservations)
	want := object(t, `apiVersion: v1
kind: ConfigMap
metadata: {name: b, namespace: default}
data: {desired: "yes", observed: preserved}`)
	require.NoError(t, reconcile.Check(result, reconcile.Expected{Resources: []*unstructured.Unstructured{want}}))
}

func TestFixtureIdentityAndUnused(t *testing.T) {
	g := generator.NewGraph("test", generator.WithNamespace("default"), generator.WithTemplate("cm", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}))
	result, err := reconcile.Run(context.Background(), reconcile.Input{Graph: g, Observations: []reconcile.Observation{{Node: "cm", Object: object(t, `metadata: {name: wrong}`)}}})
	require.ErrorContains(t, err, "identity does not match")
	require.False(t, result.Ready)
	result, err = reconcile.Run(context.Background(), reconcile.Input{Graph: g, Observations: []reconcile.Observation{{Node: "unused", Object: object(t, `data: {a: b}`)}}})
	require.NoError(t, err)
	require.Equal(t, []string{"unused[0]"}, result.UnusedObservations)
}

func TestRGD(t *testing.T) {
	var rgd v1alpha1.ResourceGraphDefinition
	require.NoError(t, yaml.Unmarshal([]byte(`apiVersion: kro.run/v1alpha1
kind: ResourceGraphDefinition
metadata: {name: test}
spec:
  schema:
    apiVersion: v1alpha1
    kind: App
    spec: {name: string}
    status: {name: "${cm.metadata.name}"}
  resources:
  - id: cm
    template:
      apiVersion: v1
      kind: ConfigMap
      metadata: {name: "${schema.spec.name}"}
`), &rgd))
	instance := object(t, `apiVersion: kro.run/v1alpha1
kind: App
metadata: {name: app, namespace: default}
spec: {name: example}`)
	result, err := reconcile.Run(context.Background(), reconcile.Input{RGD: &rgd, Instance: instance})
	require.NoError(t, err)
	require.True(t, result.Ready, result.Message)
	require.Len(t, result.Writes, 2)
	require.Equal(t, "example", result.Writes[0].Object.GetName())
	require.NoError(t, reconcile.Check(result, reconcile.Expected{Resources: []*unstructured.Unstructured{object(t, `apiVersion: kro.run/v1alpha1
kind: App
metadata: {name: app, namespace: default}
status: {name: example}`)}}))
}

func TestCustomSchemaAndStatusPatch(t *testing.T) {
	var crd apiextensions.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal([]byte(`apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: widgets.test.example}
spec:
  group: test.example
  names: {kind: Widget, plural: widgets, singular: widget}
  scope: Cluster
  versions:
  - name: v1
    served: true
    storage: true
    subresources: {status: {}}
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              name: {type: string}
          status:
            type: object
            properties:
              phase: {type: string}
`), &crd))
	g := generator.NewGraph("test", generator.WithNamespace("default"),
		generator.WithTemplate("widget", map[string]any{"apiVersion": "test.example/v1", "kind": "Widget", "metadata": map[string]any{"name": "w"}, "spec": map[string]any{"name": "example"}}),
		generator.WithPatchManifest("state", map[string]any{"apiVersion": "test.example/v1", "kind": "Widget", "metadata": map[string]any{"name": "${widget.metadata.name}"}, "status": map[string]any{"phase": "Ready"}}),
	)
	result, err := reconcile.Run(context.Background(), reconcile.Input{Graph: g, CRDs: []*apiextensions.CustomResourceDefinition{&crd}})
	require.NoError(t, err)
	require.True(t, result.Ready, result.Message)
	require.Len(t, result.Writes, 2)
	require.Equal(t, "status", result.Writes[1].Subresource)
	require.NoError(t, reconcile.Check(result, reconcile.Expected{Resources: []*unstructured.Unstructured{object(t, `apiVersion: test.example/v1
kind: Widget
metadata: {name: w}
status: {phase: Ready}`)}}))
}

func TestExistingResourceAndWaitingDependency(t *testing.T) {
	source := object(t, `apiVersion: v1
kind: ConfigMap
metadata: {name: source, namespace: default}
data: {value: observed}`)
	before := source.DeepCopy()
	g := generator.NewGraph("test", generator.WithNamespace("default"),
		generator.WithRef("source", &v1alpha1.ExternalRef{APIVersion: "v1", Kind: "ConfigMap", Metadata: v1alpha1.ExternalRefMetadata{Name: "source"}}),
		generator.WithTemplate("output", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "${source.data.value}"}}),
	)
	result, err := reconcile.Run(context.Background(), reconcile.Input{Graph: g, Resources: []*unstructured.Unstructured{source}})
	require.NoError(t, err)
	require.True(t, result.Ready, result.Message)
	require.Len(t, result.Writes, 1)
	require.Equal(t, "observed", result.Writes[0].Object.GetName())
	require.Equal(t, before, source)
	result, err = reconcile.Run(context.Background(), reconcile.Input{Graph: g, Observations: []reconcile.Observation{{Node: "output", Object: object(t, `data: {unused: value}`)}}})
	require.NoError(t, err)
	require.True(t, result.Waiting)
	require.Empty(t, result.Writes)
	require.Equal(t, []string{"output[0]"}, result.UnusedObservations)
}
