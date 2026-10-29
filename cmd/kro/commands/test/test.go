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

// Package test provides offline graph-engine tests.
package test

import (
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/kubernetes-sigs/kro/pkg/testutil/reconcile"
)

// NewCommand builds a command with its own flags so callers can reuse it.
func NewCommand() *cobra.Command {
	var filename string
	cmd := &cobra.Command{
		Use:   "test -f case.yaml",
		Short: "Run one Graph or RGD apply against static resource observations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			tc, err := reconcile.LoadCase(filename)
			if err != nil {
				return err
			}
			result, runErr := reconcile.Run(cmd.Context(), tc.Input)
			output, err := yaml.Marshal(result)
			if err != nil {
				return err
			}
			if _, err := cmd.OutOrStdout().Write(output); err != nil {
				return err
			}
			if runErr != nil {
				return runErr
			}
			if tc.Expect != nil {
				return reconcile.Check(result, *tc.Expect)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&filename, "filename", "f", "",
		"Test case with an inline or file-based Graph or RGD, observations and expectations")
	_ = cmd.MarkFlagRequired("filename")
	return cmd
}
