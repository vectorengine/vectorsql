// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"expvar"

	"base/metric"
)

var (
	metric_tcp_datablock_send_sec = "tcp:block:sendtoclient:all:sec"
)

func init() {
	expvar.Publish(metric_tcp_datablock_send_sec, metric.NewCounter("120s1s", "15m10s", "1h1m"))
}
