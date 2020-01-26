// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package counter

import (
	"sync/atomic"
	"time"
)

type Counter struct {
	name     string
	messages int64
	duration int64
	latency  int64
}

func NewCounter() *Counter {
	return &Counter{}
}

func (c *Counter) SetName(name string) {
	c.name = name
}

func (c *Counter) AddMessage(n int64) {
	atomic.AddInt64(&c.messages, n)
}

func (c *Counter) AddDuration(n time.Duration) {
	atomic.AddInt64(&c.duration, int64(n))
}

func (c *Counter) AddLatency(n time.Duration) {
	atomic.AddInt64(&c.latency, int64(n))
}

func (c *Counter) Metric() Metric {
	r1 := atomic.LoadInt64(&c.messages)
	r2 := atomic.LoadInt64(&c.duration)
	rates := float64(r1) / float64(r2/1e9)
	messages := atomic.LoadInt64(&c.messages)
	duration := atomic.LoadInt64(&c.duration)
	latency := atomic.LoadInt64(&c.latency)

	return Metric{
		Name:     c.name,
		Rates:    rates,
		Messages: messages,
		Duration: duration,
		Latency:  latency,
	}
}
