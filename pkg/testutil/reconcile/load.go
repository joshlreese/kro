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

package reconcile

import (
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// LoadCase reads a YAML or JSON test case and its referenced Graph or RGD.
// Relative paths are resolved from the test case directory.
func LoadCase(filename string) (Case, error) {
	var tc Case
	if err := readManifest(filename, &tc); err != nil {
		return Case{}, err
	}
	graphs := 0
	for _, present := range []bool{tc.Graph != nil, tc.RGD != nil, tc.GraphFile != "", tc.RGDFile != ""} {
		if present {
			graphs++
		}
	}
	if graphs != 1 {
		return Case{}, fmt.Errorf("%s: provide exactly one of graph, graphFile, rgd or rgdFile", filename)
	}
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(filepath.Dir(filename), path)
	}
	if tc.GraphFile != "" {
		tc.Graph = &v1alpha1.Graph{}
		path := resolve(tc.GraphFile)
		if err := readManifest(path, tc.Graph); err != nil {
			return Case{}, err
		}
		if tc.Graph.APIVersion != v1alpha1.GroupVersion.String() || tc.Graph.Kind != "Graph" {
			return Case{}, fmt.Errorf("%s: expected %s Graph", path, v1alpha1.GroupVersion)
		}
	}
	if tc.RGDFile != "" {
		tc.RGD = &v1alpha1.ResourceGraphDefinition{}
		path := resolve(tc.RGDFile)
		if err := readManifest(path, tc.RGD); err != nil {
			return Case{}, err
		}
		if tc.RGD.APIVersion != v1alpha1.GroupVersion.String() || tc.RGD.Kind != "ResourceGraphDefinition" {
			return Case{}, fmt.Errorf("%s: expected %s ResourceGraphDefinition", path, v1alpha1.GroupVersion)
		}
	}
	return tc, nil
}

func readManifest(filename string, out any) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	if err := yaml.UnmarshalStrict(data, out); err != nil {
		return fmt.Errorf("read %s: %w", filename, err)
	}
	return nil
}
