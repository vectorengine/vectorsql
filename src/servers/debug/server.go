// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package debug

import (
	"fmt"

	"config"

	"net/http"
	"net/http/pprof"

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

	r := http.NewServeMux()
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	go func() {
		if err := http.ListenAndServe(port, r); err != nil {
			panic(err)
		}
	}()
	log.Info("Debug Server start %v", port)
}

func (s *DebugServer) Stop() {
}
