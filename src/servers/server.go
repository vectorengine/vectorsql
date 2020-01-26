// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package servers

import (
	"config"

	"base/xlog"
	"servers/tcp"
)

type Server struct {
	log        *xlog.Log
	conf       *config.Config
	tcpHandler *tcp.TCPHandler
}

func NewServer(log *xlog.Log, conf *config.Config) *Server {
	return &Server{
		log:        log,
		conf:       conf,
		tcpHandler: tcp.NewTCPHandler(log, conf),
	}
}

func (s *Server) Start() {
	log := s.log
	tcpHandler := s.tcpHandler

	tcpHandler.Start()
	log.Info("Listening for connections with native protocol (tcp):%v", tcpHandler.Address())
}

func (s *Server) Stop() {
}
