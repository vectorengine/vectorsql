// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"expvar"

	"base/metric"
)

var (
	metric_function_tvf_range_sec      = "function:tvf:range:sec"
	metric_function_tvf_rangetable_sec = "function:tvf:rangetable:sec"
	metric_function_tvf_randtable_sec  = "function:tvf:randtable:sec"
	metric_function_tvf_zip_sec        = "function:tvf:zip:sec"
)

func init() {
	expvar.Publish(metric_function_tvf_range_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
	expvar.Publish(metric_function_tvf_rangetable_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
	expvar.Publish(metric_function_tvf_randtable_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
	expvar.Publish(metric_function_tvf_zip_sec, metric.NewGauge("120s1s", "15m10s", "1h1m"))
}
