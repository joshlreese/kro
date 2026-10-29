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

// Package reconcile runs one graph-engine apply against an in-memory client.
// It does not contact a cluster or run other controllers.
package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
	graphruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// Input supplies either Graph or RGD and Instance. Resources exist before the
// apply starts. Observations appear only when a template's identity resolves.
// CRDs supply schemas and resource scopes for custom APIs.
type Input struct {
	Graph        *v1alpha1.Graph                           `json:"graph,omitempty"`
	RGD          *v1alpha1.ResourceGraphDefinition         `json:"rgd,omitempty"`
	Instance     *unstructured.Unstructured                `json:"instance,omitempty"`
	Resources    []*unstructured.Unstructured              `json:"resources,omitempty"`
	Observations []Observation                             `json:"observations,omitempty"`
	CRDs         []*apiextensions.CustomResourceDefinition `json:"crds,omitempty"`
}

// Observation provides existing state for a template node. Node uses paths
// such as "database" or "backend/database" for nested graphs. Index selects
// a collection item, starting at zero. Omitted identity fields are filled from
// the resolved template; supplied identity fields must match it.
type Observation struct {
	Node   string                     `json:"node"`
	Index  int                        `json:"index,omitempty"`
	Object *unstructured.Unstructured `json:"object"`
}

// Write records an attempted write before the fake client changes the object.
// Error is populated when the write failed.
type Write struct {
	Operation   string                     `json:"operation"`
	Subresource string                     `json:"subresource,omitempty"`
	Object      *unstructured.Unstructured `json:"object"`
	Error       string                     `json:"error,omitempty"`
}

// Result contains the apply outcome and the resulting resources. Waiting is a
// normal outcome when observations do not yet satisfy readiness or data needs.
// UnusedObservations lists fixtures whose template identities did not resolve.
type Result struct {
	Ready              bool                         `json:"ready"`
	Waiting            bool                         `json:"waiting"`
	Message            string                       `json:"message,omitempty"`
	Writes             []Write                      `json:"writes"`
	Resources          []*unstructured.Unstructured `json:"resources"`
	Unresolved         []string                     `json:"unresolved,omitempty"`
	UnusedObservations []string                     `json:"unusedObservations,omitempty"`
}

// Run compiles and applies exactly once. It returns partial results with an
// error if compilation, a fixture, or a write fails. It never retries a graph.
// Inputs are copied. Fake-client behavior does not include API admission,
// defaulting, RBAC, or other controllers. Custom API apply uses deduced types.
func Run(ctx context.Context, in Input) (Result, error) {
	result := Result{Writes: []Write{}, Resources: []*unstructured.Unstructured{}}
	if (in.Graph == nil) == (in.RGD == nil) {
		return result, fmt.Errorf("provide either graph or rgd")
	}
	if (in.RGD != nil) != (in.Instance != nil) {
		return result, fmt.Errorf("rgd requires instance; graph does not accept instance")
	}
	sr, mapper, err := offlineSchemas(in.CRDs)
	if err != nil {
		return result, err
	}
	if in.RGD != nil {
		in.RGD = in.RGD.DeepCopy()
		if in.RGD.Spec.Schema != nil && in.RGD.Spec.Schema.Group == "" {
			in.RGD.Spec.Schema.Group = v1alpha1.GroupVersion.Group
		}
		builder, e := graph.NewBuilder(nil, nil, graph.WithSchemaResolver(sr), graph.WithRESTMapper(mapper))
		if e != nil {
			return result, e
		}
		built, e := builder.NewResourceGraphDefinition(in.RGD.DeepCopy(), graph.Config{MaxCollectionSize: 1000, MaxCollectionDimensionSize: 10})
		if e != nil {
			return result, e
		}
		if in.Instance.GetAPIVersion() != built.CRD.Spec.Group+"/"+built.CRD.Spec.Versions[0].Name || in.Instance.GetKind() != built.CRD.Spec.Names.Kind {
			return result, fmt.Errorf("instance apiVersion and kind do not match the RGD")
		}
		in.CRDs = append(append([]*apiextensions.CustomResourceDefinition{}, in.CRDs...), built.CRD)
		sr, mapper, err = offlineSchemas(in.CRDs)
		if err != nil {
			return result, err
		}
	}
	comp := compiler.NewCompilerWithDependencies(sr, mapper)
	var rt *graphruntime.Runtime
	if in.Graph != nil {
		g := in.Graph.DeepCopy()
		p, e := comp.CompileWithOptions(g)
		if e != nil {
			return result, e
		}
		rt = graphruntime.New(p, g)
	} else {
		rt, _, err = rgdadapter.BuildRuntimeForInstance(in.RGD.DeepCopy(), in.Instance.DeepCopy(), comp)
		if err != nil {
			return result, err
		}
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return result, err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return result, err
	}
	state := &runState{observations: map[string]Observation{}, used: map[string]bool{}, identities: map[string]*unstructured.Unstructured{}}
	for _, o := range in.Observations {
		key := observationKey(o.Node, o.Index)
		if o.Node == "" || o.Index < 0 || o.Object == nil {
			return result, fmt.Errorf("invalid observation %q", key)
		}
		if _, ok := state.observations[key]; ok {
			return result, fmt.Errorf("duplicate observation %q", key)
		}
		o.Object = o.Object.DeepCopy()
		state.observations[key] = o
	}
	objects := []client.Object{}
	statusObjects := []client.Object{}
	for _, obj := range in.Resources {
		if obj == nil || obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
			return result, fmt.Errorf("resources require apiVersion, kind and metadata.name")
		}
		if _, ok := state.identities[identity(obj)]; ok {
			return result, fmt.Errorf("duplicate resource %s", identity(obj))
		}
		objects = append(objects, obj.DeepCopy())
		state.remember(obj)
		if _, ok := obj.Object["status"]; ok {
			statusObjects = append(statusObjects, obj)
		}
	}
	if in.Instance != nil {
		objects = append(objects, in.Instance.DeepCopy())
		statusObjects = append(statusObjects, in.Instance)
		state.remember(in.Instance)
	}
	for _, o := range in.Observations {
		if _, ok := o.Object.Object["status"]; ok && o.Object.GetKind() != "" && o.Object.GetAPIVersion() != "" {
			statusObjects = append(statusObjects, o.Object)
		}
	}
	for _, crd := range in.CRDs {
		for _, v := range crd.Spec.Versions {
			if v.Subresources != nil && v.Subresources.Status != nil {
				o := &unstructured.Unstructured{}
				o.SetAPIVersion(crd.Spec.Group + "/" + v.Name)
				o.SetKind(crd.Spec.Names.Kind)
				statusObjects = append(statusObjects, o)
			}
		}
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(objects...).WithStatusSubresource(statusObjects...).Build()
	state.client = base
	cl := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			state.mu.Lock()
			err := state.err
			state.mu.Unlock()
			if err != nil {
				return err
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			w := Write{Operation: "patch", Object: obj.(*unstructured.Unstructured).DeepCopy()}
			if p.Type() == client.Apply.Type() {
				w.Operation = "apply"
			}
			err := c.Patch(ctx, obj, p, opts...)
			state.record(w, err)
			return err
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			w := Write{Operation: "patch", Subresource: sub, Object: obj.(*unstructured.Unstructured).DeepCopy()}
			if p.Type() == client.Apply.Type() {
				w.Operation = "apply"
			}
			err := c.SubResource(sub).Patch(ctx, obj, p, opts...)
			state.record(w, err)
			return err
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			w := Write{Operation: "update", Subresource: sub, Object: obj.(*unstructured.Unstructured).DeepCopy()}
			err := c.SubResource(sub).Update(ctx, obj, opts...)
			state.record(w, err)
			return err
		},
	})
	exec := executor.NewSimple(cl)
	exec.ApplyConcurrency = 1
	exec.ConflictDetection = in.Graph != nil
	exec.GateReadiness = in.RGD != nil
	exec.LabelInjector = func(obj *unstructured.Unstructured) {
		if in.Graph != nil && obj.GetLabels()[metadata.CollectionIndexLabel] != "" && in.Graph.UID != "" {
			labels := obj.GetLabels()
			if _, ok := labels[metadata.InstanceIDLabel]; !ok {
				labels[metadata.InstanceIDLabel] = string(in.Graph.UID)
				obj.SetLabels(labels)
			}
		}
		state.observe(ctx, obj)
	}
	applied, applyErr := exec.Apply(ctx, rt, watchrouter.NoopWatcher{})
	result.Unresolved = applied.Unresolved
	result.Waiting = errors.Is(applyErr, executor.ErrNotReady)
	result.Ready = applyErr == nil
	if applyErr != nil {
		result.Message = applyErr.Error()
	}
	result.Writes = state.writes
	for key := range state.observations {
		if !state.used[key] {
			result.UnusedObservations = append(result.UnusedObservations, key)
		}
	}
	sort.Strings(result.UnusedObservations)
	keys := make([]string, 0, len(state.identities))
	for key := range state.identities {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		obj := state.identities[key].DeepCopy()
		if err := base.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return result, err
		}
		result.Resources = append(result.Resources, obj)
	}
	if state.err != nil {
		result.Ready = false
		return result, state.err
	}
	if result.Waiting {
		return result, nil
	}
	return result, applyErr
}

type runState struct {
	mu           sync.Mutex
	client       client.Client
	observations map[string]Observation
	used         map[string]bool
	identities   map[string]*unstructured.Unstructured
	writes       []Write
	err          error
}

func identity(obj *unstructured.Unstructured) string {
	return obj.GetAPIVersion() + "/" + obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
}
func observationKey(node string, index int) string { return node + "[" + strconv.Itoa(index) + "]" }
func (s *runState) remember(obj *unstructured.Unstructured) {
	s.identities[identity(obj)] = obj.DeepCopy()
}
func (s *runState) record(w Write, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		w.Error = err.Error()
	}
	s.writes = append(s.writes, w)
	s.remember(w.Object)
}

func (s *runState) observe(ctx context.Context, desired *unstructured.Unstructured) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node := desired.GetAnnotations()[metadata.NodePathAnnotation]
	if node == "" {
		return
	}
	index := desired.GetLabels()[metadata.CollectionIndexLabel]
	if index == "" {
		index = "0"
	}
	key := node + "[" + index + "]"
	fixture, ok := s.observations[key]
	if !ok || s.used[key] {
		return
	}
	s.used[key] = true
	obj := fixture.Object.DeepCopy()
	if (obj.GetAPIVersion() != "" && obj.GetAPIVersion() != desired.GetAPIVersion()) || (obj.GetKind() != "" && obj.GetKind() != desired.GetKind()) || (obj.GetName() != "" && obj.GetName() != desired.GetName()) || (obj.GetNamespace() != "" && obj.GetNamespace() != desired.GetNamespace()) {
		s.err = errors.Join(s.err, fmt.Errorf("observation %s identity does not match %s", key, identity(desired)))
		return
	}
	obj.SetAPIVersion(desired.GetAPIVersion())
	obj.SetKind(desired.GetKind())
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())
	if err := s.client.Create(ctx, obj); err != nil {
		s.err = errors.Join(s.err, fmt.Errorf("introduce observation %s: %w", key, err))
		return
	}
	s.remember(obj)
}

// UnmarshalJSON allows an observation to omit identity fields filled by Run.
func (o *Observation) UnmarshalJSON(data []byte) error {
	var fields struct {
		Node   string         `json:"node"`
		Index  int            `json:"index,omitempty"`
		Object map[string]any `json:"object"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fields); err != nil {
		return err
	}
	o.Node = fields.Node
	o.Index = fields.Index
	if fields.Object != nil {
		o.Object = &unstructured.Unstructured{Object: fields.Object}
	}
	return nil
}
