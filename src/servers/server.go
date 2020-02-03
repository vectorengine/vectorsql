// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package servers

import (
	"config"

	"base/xlog"
	"servers/debug"
	"servers/http"
	"servers/tcp"
)

type Server struct {
	log         *xlog.Log
	conf        *config.Config
	tcpServer   *tcp.TCPHandler
	httpServer  *http.HTTPHandler
	debugServer *debug.DebugServer
}

func NewServer(log *xlog.Log, conf *config.Config) *Server {
	return &Server{
		log:         log,
		conf:        conf,
		tcpServer:   tcp.NewTCPHandler(log, conf),
		httpServer:  http.NewHTTPHandler(log, conf),
		debugServer: debug.NewDebugServer(log, conf),
	}
}

func (s *Server) Start() {
	log := s.log
	s.debugServer.Start()

	s.tcpServer.Start()
	s.httpServer.Start()
	log.Info("Listening for connections with native protocol (tcp):%v", s.tcpServer.Address())
}

func (s *Server) Stop() {
}
