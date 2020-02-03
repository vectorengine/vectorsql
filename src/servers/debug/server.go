// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package debug

import (
	"expvar"
	"fmt"
	"runtime"
	"time"

	"net/http"
	_ "net/http/pprof"

	"config"

	"base/metric"
	"base/xlog"
)

type DebugServer struct {
	log  *xlog.Log
	conf *config.Config
}

func NewDebugServer(log *xlog.Log, conf *config.Config) *DebugServer {
	return &DebugServer{
		log:  log,
		conf: conf,
	}
}

func (s *DebugServer) Start() {
	log := s.log
	port := fmt.Sprintf(":%v", s.conf.Server.DebugPort)

	// Some Go internal metrics.
	expvar.Publish("#go:numgoroutine", metric.NewGauge("2m1s", "15m30s", "1h1m"))
	expvar.Publish("#go:numcgocall", metric.NewGauge("2m1s", "15m30s", "1h1m"))
	expvar.Publish("#go:alloc", metric.NewGauge("2m1s", "15m30s", "1h1m"))
	expvar.Publish("#go:alloctotal", metric.NewGauge("2m1s", "15m30s", "1h1m"))
	expvar.Publish("#go:heapobjects", metric.NewGauge("2m1s", "15m30s", "1h1m"))
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			m := &runtime.MemStats{}
			runtime.ReadMemStats(m)
			expvar.Get("#go:numgoroutine").(metric.Metric).Add(float64(runtime.NumGoroutine()))
			expvar.Get("#go:numcgocall").(metric.Metric).Add(float64(runtime.NumCgoCall()))
			expvar.Get("#go:alloc").(metric.Metric).Add(float64(m.Alloc) / 1000000)
			expvar.Get("#go:alloctotal").(metric.Metric).Add(float64(m.TotalAlloc) / 1000000)
			expvar.Get("#go:heapobjects").(metric.Metric).Add(float64(m.HeapObjects))
		}
	}()
	http.Handle("/debug/metrics", metric.Handler(metric.Exposed))

	go func() {
		if err := http.ListenAndServe(port, nil); err != nil {
			panic(err)
		}
	}()
	log.Info("Debug Server start %v", port)
}

func (s *DebugServer) Stop() {
}
