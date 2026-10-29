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

package test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleAndFailedAssertion(t *testing.T) {
	data, err := os.ReadFile("../../../../examples/testing/observed-deployment.yaml")
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		data    []byte
		failure string
	}{
		{"passing", data, ""},
		{"assertion", []byte(strings.Replace(string(data), "available: \"3\"", "available: \"9\"", 1)), "data.available: expected 9, got 3"},
		{"unknown field", append(append([]byte{}, data...), []byte("unexpected: true\n")...), "unknown field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "case.yaml")
			require.NoError(t, os.WriteFile(filename, tc.data, 0600))
			cmd := NewCommand()
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs([]string{"-f", filename})
			err := cmd.Execute()
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
			} else {
				require.NoError(t, err)
				require.Contains(t, output.String(), "ready: true")
				require.Contains(t, output.String(), "operation: apply")
			}
		})
	}
}
