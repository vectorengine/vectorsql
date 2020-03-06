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

func NewResult() *Result {
	return &Result{}
}

func (r *Result) SetInput(in processors.IProcessor) {
	r.In = in
}

func (r *Result) SetOutput(out datastreams.IDataBlockOutputStream) {
	r.Out = out
}

func (r *Result) Read() <-chan interface{} {
	return r.In.In().Recv()
}
