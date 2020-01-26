// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"time"
)

type MockSleepTransform struct {
	ms int
	BaseProcessor
}

func NewMockSleepTransform(name string, ms int) IProcessor {
	return &MockSleepTransform{
		ms:            ms,
		BaseProcessor: NewBaseProcessor(name),
	}
}

func (p *MockSleepTransform) Execute() {
	onNext := func(x interface{}) {
		start := time.Now()
		time.Sleep(time.Millisecond * time.Duration(p.ms))
		p.AddMessage(1)
		p.AddDuration(time.Since(start))
		p.Out().Send(x)
	}
	p.Subscribe(onNext)
}

type MockAddTransform struct {
	BaseProcessor
}

func NewMockAddTransform(name string) IProcessor {
	return &MockAddTransform{
		BaseProcessor: NewBaseProcessor(name),
	}
}

func (p *MockAddTransform) Execute() {
	onNext := func(x interface{}) {
		p.AddMessage(1)
		switch x := x.(type) {
		case int:
			x = x + 1
			p.Out().Send(x)
		case error:
			p.Out().Send(x)
		}
	}
	p.Subscribe(onNext)
}

type MockMultiTransform struct {
	BaseProcessor
}

func NewMockMultiTransform(name string) IProcessor {
	return &MockMultiTransform{
		BaseProcessor: NewBaseProcessor(name),
	}
}

func (p *MockMultiTransform) Execute() {
	onNext := func(x interface{}) {
		p.AddMessage(1)
		switch x := x.(type) {
		case int:
			x = x * 4
			p.Out().Send(x)
		case error:
			p.Out().Send(x)
		}
	}
	p.Subscribe(onNext)
}
