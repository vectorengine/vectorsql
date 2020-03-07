// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"executors"
)

type QueryState struct {
	result *executors.Result
}

func (state *QueryState) SetExecutorResult(r *executors.Result) {
	state.result = r
}

func (state *QueryState) Empty() bool {
	return state.result == nil
}

func (state *QueryState) Reset() {
	state.result = nil
}
