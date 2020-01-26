// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"net"

	"datablocks"
	"datastreams"
	"sessions"

	"base/binary"
	"base/errors"
	"servers/protocol"
)

type TCPSession struct {
	conn    net.Conn
	hello   *protocol.HelloProtocol
	reader  *binary.Reader
	writer  *binary.Writer
	session *sessions.Session
}

func NewTCPSession(conn net.Conn) *TCPSession {
	return &TCPSession{
		conn:    conn,
		reader:  binary.NewReader(conn),
		writer:  binary.NewWriter(conn),
		session: sessions.NewSession(),
	}
}

func (session *TCPSession) sendException(x error, withStack bool) error {
	writer := session.writer

	if err := protocol.WriteExceptionResponse(writer, x, withStack); err != nil {
		return err
	}
	return session.flush()
}

func (session *TCPSession) sendData(block *datablocks.DataBlock) error {
	writer := session.writer
	output := datastreams.NewNativeBlockOutputStream(writer)

	if err := writer.Uvarint(uint64(protocol.ServerData)); err != nil {
		return errors.Wrapf(err, "Couldn't write query header")
	}
	if err := writer.String(""); err != nil {
		return err
	}
	if err := output.Write(block); err != nil {
		return err
	}
	return nil
}

func (session *TCPSession) sendEndOfStream() error {
	writer := session.writer

	if err := writer.Uvarint(uint64(protocol.ServerEndOfStream)); err != nil {
		return errors.Wrapf(err, "Couldn't write ServerEndOfStream")
	}
	return session.flush()
}

func (session *TCPSession) flush() error {
	writer := session.writer

	if err := writer.Flush(); err != nil {
		return errors.Wrapf(err, "Couldn't flush")
	}
	return nil
}
