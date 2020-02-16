// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"

	"base/metric"
)

var (
	metric_datablock_split_sec   = "datablock:split:sec"
	metric_datablock_orderby_sec = "datablock:orderby:sec"
	metric_datablock_filter_sec  = "datablock:filter:sec"
)

func init() {
	expvar.Publish(metric_datablock_split_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
	expvar.Publish(metric_datablock_orderby_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
	expvar.Publish(metric_datablock_filter_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
}
