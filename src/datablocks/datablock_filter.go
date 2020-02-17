// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"time"

	"base/metric"
	"datavalues"
)

func (block *DataBlock) Filter(checks []*datavalues.Value) error {
	defer expvar.Get(metric_datablock_filter_sec).(metric.Metric).Record(time.Now())

	// In place filter.
	n := 0
	seqs := block.seqs
	for i, check := range checks {
		if check.AsBool() {
			seqs[n] = seqs[i]
			n++
		}
	}
	block.seqs = seqs[:n]
	return nil
}
