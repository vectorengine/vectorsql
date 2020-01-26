// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"fmt"

	"base/errors"
	"servers/protocol"
)

func (s *TCPHandler) processHello(session *TCPSession) error {
	var err error
	log := s.log
	conf := s.conf
	reader := session.reader
	writer := session.writer

	// Check packet type.
	var packetType uint64
	if packetType, err = reader.Uvarint(); err != nil {
		return errors.Wrapf(err, "Hello packet type")
	}

	if packetType != uint64(protocol.ClientHello) {
		if packetType == uint64('G') || packetType == uint64('P') {
			warning := fmt.Sprintf("HTTP/1.0 400 Bad Request\r\n\r\n")
			if err := writer.String(warning); err != nil {
				return err
			}
			if err := session.flush(); err != nil {
				return err
			}
			return errors.Wrapf(errors.New("HTTP request wrong port"), "")
		}
	}

	// Request.
	if session.hello, err = protocol.ReadHelloRequest(reader); err != nil {
		return err
	}
	// Set the session database.
	if session.hello.Database != "" {
		session.session.SetDatabase(session.hello.Database)
	}

	log.Debug("Receive client hello:%+v", session.hello)
	// Response.
	if err := protocol.WriteHelloResponse(writer, session.hello.ClientRevision, conf.Server.DisplayName); err != nil {
		return err
	}
	// flush.
	return session.flush()
}

func (s *TCPHandler) processUnexceptedHello(session *TCPSession) error {
	conf := s.conf
	return session.sendException(errors.ErrorWithCode(
		errors.UNEXPECTED_PACKET_FROM_CLIENT,
		"Unexpected packet Hello received from client"),
		conf.Server.CalculateTextStackTrace)
}
