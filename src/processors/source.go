// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import ()

type Source struct {
	BaseProcessor
}

func NewSource(name string) IProcessor {
	return &Source{
		BaseProcessor: NewBaseProcessor(name),
	}
}

func (p *Source) Out() *OutPort {
	return p.BaseProcessor.Out()
}
