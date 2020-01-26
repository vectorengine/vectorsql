// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"fmt"
	"net"

	"config"

	"base/errors"
	"base/xlog"
	"servers/protocol"
)

type TCPHandler struct {
	log      *xlog.Log
	conf     *config.Config
	listener net.Listener
}

func NewTCPHandler(log *xlog.Log, conf *config.Config) *TCPHandler {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", conf.Server.ListenHost, conf.Server.TCPPort))
	if err != nil {
		log.Panic("Couldn't listen: %+v", err)
	}
	return &TCPHandler{
		log:      log,
		conf:     conf,
		listener: listener,
	}
}

func (s *TCPHandler) Start() {
	log := s.log

	go func(svr *TCPHandler) {
		for {
			conn, err := svr.listener.Accept()
			if err != nil {
				log.Panic("Couldn't accept: %+v", err)
			}
			go svr.handle(conn)
		}
	}(s)
}

func (s *TCPHandler) Stop() {
}

func (s *TCPHandler) Address() string {
	return fmt.Sprintf(":%v", s.conf.Server.TCPPort)
}

func (s *TCPHandler) handle(conn net.Conn) {
	log := s.log

	// Catch panics, and close the connection in any case.
	defer func() {
		conn.Close()
		if x := recover(); x != nil {
			log.Error("%+v", errors.Errorf("%+v", x))
		}
	}()

	session := NewTCPSession(conn)
	log.Debug("Connection coming:%s", conn.RemoteAddr().String())
	if err := s.handlePacket(session); err != nil {
		log.Error("%+v, %T", err, err)
		conn.Close()
	}
}

func (s *TCPHandler) handlePacket(session *TCPSession) error {
	var err error
	var packetType uint64
	log := s.log

	// hello.
	if err := s.processHello(session); err != nil {
		return err
	}

	// packets.
	for {
		if packetType, err = session.reader.Uvarint(); err != nil {
			return err
		}
		log.Debug("Receive packet type:%v", protocol.ClientPacketType(packetType))
		switch packetType {
		case protocol.ClientPing:
			if err := s.processPing(session); err != nil {
				return err
			}
		case protocol.ClientQuery:
			if err := s.processQuery(session); err != nil {
				return err
			}
		case protocol.ClientData:
			if err := s.processData(session); err != nil {
				return err
			}
		case protocol.ClientHello:
			if err := s.processUnexceptedHello(session); err != nil {
				return err
			}
		default:
			return errors.Errorf("Unhandle packet type:%v", protocol.ClientPacketType(packetType))
		}
	}
}
