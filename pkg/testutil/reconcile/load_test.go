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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubernetes-sigs/kro/pkg/testutil/reconcile"
)

func TestLoadCase(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "tests"), 0700))
	graph := `apiVersion: kro.run/v1alpha1
kind: Graph
metadata: {name: project, namespace: default}
spec:
  nodes:
  - id: cm
    template:
      apiVersion: v1
      kind: ConfigMap
      metadata: {name: from-project}
`
	rgd := `apiVersion: kro.run/v1alpha1
kind: ResourceGraphDefinition
metadata: {name: project}
spec:
  schema:
    apiVersion: v1alpha1
    kind: App
    spec: {name: string}
  resources:
  - id: cm
    template:
      apiVersion: v1
      kind: ConfigMap
      metadata: {name: "${schema.spec.name}"}
`
	for name, data := range map[string]string{"graph.yaml": graph, "rgd.yaml": rgd, "invalid.yaml": "spec: [", "wrong-kind.yaml": "apiVersion: kro.run/v1alpha1\nkind: ConfigMap\n"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(data), 0600))
	}
	for _, tc := range []struct{ name, data, failure string }{
		{"relative graph", "graphFile: ../graph.yaml", ""},
		{"absolute graph", "graphFile: " + filepath.Join(dir, "graph.yaml"), ""},
		{"JSON case", `{"graphFile":"../graph.yaml"}`, ""},
		{"relative RGD", `rgdFile: ../rgd.yaml
instance:
  apiVersion: kro.run/v1alpha1
  kind: App
  metadata: {name: app, namespace: default}
  spec: {name: from-project}
`, ""},
		{"inline graph", "graph:\n  " + strings.ReplaceAll(strings.TrimSpace(graph), "\n", "\n  "), ""},
		{"inline and file", "graph: {}\ngraphFile: ../graph.yaml", "exactly one"},
		{"both files", "graphFile: ../graph.yaml\nrgdFile: ../rgd.yaml", "exactly one"},
		{"inline RGD and graph file", "rgd: {}\ngraphFile: ../graph.yaml", "exactly one"},
		{"missing file", "graphFile: ../missing.yaml", "missing.yaml"},
		{"malformed manifest", "graphFile: ../invalid.yaml", "invalid.yaml"},
		{"wrong kind", "graphFile: ../wrong-kind.yaml", "expected kro.run/v1alpha1 Graph"},
		{"wrong RGD kind", "rgdFile: ../wrong-kind.yaml", "expected kro.run/v1alpha1 ResourceGraphDefinition"},
		{"no graph", "{}", "exactly one"},
		{"unknown field", "graphFile: ../graph.yaml\nunknown: true", "unknown field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, "tests", "case.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.data), 0600))
			loaded, err := reconcile.LoadCase(path)
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			result, err := reconcile.Run(context.Background(), loaded.Input)
			require.NoError(t, err)
			require.True(t, result.Ready, result.Message)
			require.NotEmpty(t, result.Writes)
			require.Equal(t, "from-project", result.Writes[0].Object.GetName())
		})
	}
}
