// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package counter

import (
	"fmt"
)

type Metric struct {
	Name     string  `json:"name"`
	Rates    float64 `json:"rates"`
	Messages int64   `json:"messages"`
	Duration int64   `json:"duration"`
	Latency  int64   `json:"latency"`
}

func (m Metric) String() string {
	return fmt.Sprintf("{Name:%v, \tMessage:%+v,\tDuration:%.2f(ms),\tLatency:%.2f(ms),\tRates:%.2f messages/s}",
		m.Name, m.Messages, float64(m.Duration/1e6), float64(m.Latency/1e6), m.Rates)
}
