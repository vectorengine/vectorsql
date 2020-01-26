// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"context"

	"base/counter"
)

type IProcessor interface {
	Name() string
	Pause()
	Resume()
	Execute()
	In() *InPort
	Out() *OutPort
	To(...IProcessor)
	From(...IProcessor)
	Metric() counter.Metric
	SetContext(context.Context)
}
