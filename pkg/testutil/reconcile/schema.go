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
	"strings"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/discovery"
	clientopenapi "k8s.io/client-go/openapi"
	"k8s.io/client-go/openapi/openapitest"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

type schemas struct {
	builtin resolver.SchemaResolver
	custom  map[schema.GroupVersionKind]*spec.Schema
}

func (s *schemas) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	if v, ok := s.custom[gvk]; ok {
		return v, nil
	}
	v, err := s.builtin.ResolveSchema(gvk)
	if err != nil {
		return nil, fmt.Errorf("schema for %s: supply its CRD: %w", gvk, err)
	}
	return v, nil
}

func offlineSchemas(crds []*apiextensions.CustomResourceDefinition) (*schemas, *meta.DefaultRESTMapper, error) {
	s := &schemas{builtin: &resolver.ClientDiscoveryResolver{Discovery: embeddedDiscovery{}}, custom: map[schema.GroupVersionKind]*spec.Schema{}}
	mapper := meta.NewDefaultRESTMapper(nil)
	paths, err := openapitest.NewEmbeddedFileClient().Paths()
	if err != nil {
		return nil, nil, err
	}
	for _, file := range paths {
		data, err := file.Schema("application/json")
		if err != nil {
			return nil, nil, err
		}
		var document struct {
			Paths map[string]struct {
				Get struct {
					GVK schema.GroupVersionKind `json:"x-kubernetes-group-version-kind"`
				} `json:"get"`
			} `json:"paths"`
		}
		if err := json.Unmarshal(data, &document); err != nil {
			return nil, nil, err
		}
		for path, operations := range document.Paths {
			gvk := operations.Get.GVK
			if gvk.Kind == "" || !strings.HasSuffix(path, "/{name}") || strings.Contains(path, "/watch/") {
				continue
			}
			parts := strings.Split(strings.TrimSuffix(path, "/{name}"), "/")
			scope := meta.RESTScopeRoot
			if strings.Contains(path, "/{namespace}/") {
				scope = meta.RESTScopeNamespace
			}
			plural := gvk.GroupVersion().WithResource(parts[len(parts)-1])
			singular := gvk.GroupVersion().WithResource(strings.ToLower(gvk.Kind))
			mapper.AddSpecific(gvk, plural, singular, scope)
		}
	}
	for _, crd := range crds {
		if crd == nil {
			return nil, nil, fmt.Errorf("nil CRD")
		}
		scope := meta.RESTScopeNamespace
		if crd.Spec.Scope == apiextensions.ClusterScoped {
			scope = meta.RESTScopeRoot
		}
		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}
			if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
				return nil, nil, fmt.Errorf("CRD %s version %s has no schema", crd.Name, v.Name)
			}
			data, err := json.Marshal(v.Schema.OpenAPIV3Schema)
			if err != nil {
				return nil, nil, err
			}
			var sch spec.Schema
			if err := json.Unmarshal(data, &sch); err != nil {
				return nil, nil, err
			}
			// CRD schemas may leave out the fields Kubernetes adds to every resource.
			object, err := s.builtin.ResolveSchema(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
			if err != nil {
				return nil, nil, err
			}
			if sch.Properties == nil {
				sch.Properties = map[string]spec.Schema{}
			}
			for _, name := range []string{"apiVersion", "kind", "metadata"} {
				sch.Properties[name] = object.Properties[name]
			}
			gvk := schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Spec.Names.Kind}
			s.custom[gvk] = &sch
			mapper.AddSpecific(gvk, gvk.GroupVersion().WithResource(crd.Spec.Names.Plural), gvk.GroupVersion().WithResource(crd.Spec.Names.Singular), scope)
		}
	}
	return s, mapper, nil
}

// embeddedDiscovery serves client-go's bundled OpenAPI test snapshots offline.
type embeddedDiscovery struct{ discovery.DiscoveryInterface }

func (embeddedDiscovery) OpenAPIV3() clientopenapi.Client { return openapitest.NewEmbeddedFileClient() }
