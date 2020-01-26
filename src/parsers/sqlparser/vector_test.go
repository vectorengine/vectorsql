/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sqlparser

import (
	"testing"
)

func TestVectors(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "create database t1 engine=xx",
		output: "create database t1 ENGINE = xx",
	}, {
		input:  "create database t1",
		output: "create database t1",
	}, {
		input:  "create table t1(a int) engine=xx",
		output: "create table t1 (\n\ta int\n)ENGINE = xx",
	}, {
		input:  "create table t1(a int)",
		output: "create table t1 (\n\ta int\n)",
	}, {
		input:  "create table t1(a int) engine=File(JSON)",
		output: "create table t1 (\n\ta int\n)ENGINE = File(JSON)",
	}}
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}
}
