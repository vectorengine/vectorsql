// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"context"
	"time"
)

type IProcessor interface {
	Name() string
	Pause()
	Resume()
	Execute()
	In() *InPort
	Out() *OutPort
	Duration() time.Duration
	To(...IProcessor)
	From(...IProcessor)
	SetContext(context.Context)
}
