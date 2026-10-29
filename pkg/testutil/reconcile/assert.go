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
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Case is a portable test: inputs followed by optional expectations.
type Case struct {
	Input  `json:",inline"`
	Expect *Expected `json:"expect,omitempty"`
}

// Expected checks readiness and resource fields. Extra object fields are
// allowed; arrays must match in length and order. Resources need a full
// identity so assertions cannot accidentally match a different object.
type Expected struct {
	Ready     *bool                        `json:"ready,omitempty"`
	Waiting   *bool                        `json:"waiting,omitempty"`
	Resources []*unstructured.Unstructured `json:"resources,omitempty"`
}

// Check compares an outcome with expected fields. It ignores unspecified
// fields, including server-generated metadata, without changing the result.
func Check(result Result, expected Expected) error {
	if expected.Ready != nil && *expected.Ready != result.Ready {
		return fmt.Errorf("ready: expected %t, got %t", *expected.Ready, result.Ready)
	}
	if expected.Waiting != nil && *expected.Waiting != result.Waiting {
		return fmt.Errorf("waiting: expected %t, got %t", *expected.Waiting, result.Waiting)
	}
	actual := map[string]*unstructured.Unstructured{}
	for _, obj := range result.Resources {
		actual[identity(obj)] = obj
	}
	for _, want := range expected.Resources {
		if want == nil || want.GetAPIVersion() == "" || want.GetKind() == "" || want.GetName() == "" {
			return fmt.Errorf("expected resources require apiVersion, kind and metadata.name")
		}
		got, ok := actual[identity(want)]
		if !ok {
			return fmt.Errorf("expected resource %s is absent", identity(want))
		}
		// JSON normalization makes numeric comparisons independent of Go number types.
		var w, g any
		wb, err := json.Marshal(want.Object)
		if err != nil {
			return err
		}
		gb, err := json.Marshal(got.Object)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(wb, &w); err != nil {
			return err
		}
		if err := json.Unmarshal(gb, &g); err != nil {
			return err
		}
		if err := contains(g, w, identity(want)); err != nil {
			return err
		}
	}
	return nil
}

func contains(got, want any, path string) error {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected an object, got %v", path, got)
		}
		keys := make([]string, 0, len(w))
		for k := range w {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v, ok := g[k]
			if !ok {
				return fmt.Errorf("%s.%s: missing field", path, k)
			}
			if err := contains(v, w[k], path+"."+k); err != nil {
				return err
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return fmt.Errorf("%s: expected array %v, got %v", path, w, got)
		}
		for i := range w {
			if err := contains(g[i], w[i], fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	default:
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("%s: expected %v, got %v", path, want, got)
		}
	}
	return nil
}
