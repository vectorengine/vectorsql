// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"sync"
)

type OutPort struct {
	mu     sync.Mutex
	name   string
	edges  []*InPort
	closed bool
}

func NewOutPort(name string) *OutPort {
	return &OutPort{name: name}
}

func (pt *OutPort) Name() string {
	return pt.name
}

func (pt *OutPort) AddEdge(rpt *InPort) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.edges = append(pt.edges, rpt)
}

func (pt *OutPort) To(rpt *InPort) {
	rpt.AddEdge(pt)
	pt.AddEdge(rpt)
}

func (pt *OutPort) Send(v interface{}) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, rpt := range pt.edges {
		rpt.Send(v)
	}
}

func (pt *OutPort) Close() {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if !pt.closed {
		for _, rpt := range pt.edges {
			rpt.Close()
		}
		pt.closed = true
	}
}
