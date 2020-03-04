// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"datastreams"
	"processors"
)

type IExecutor interface {
	String() string
	Execute() (*Result, error)
}

type Result struct {
	In  processors.IProcessor
	Out datastreams.IDataBlockOutputStream
}

func NewResult(in processors.IProcessor, out datastreams.IDataBlockOutputStream) *Result {
	return &Result{In: in, Out: out}
}

func (r *Result) Read() <-chan interface{} {
	return r.In.In().Recv()
}
