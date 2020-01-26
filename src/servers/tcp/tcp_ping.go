// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"servers/protocol"
)

func (s *TCPHandler) processPing(session *TCPSession) error {
	log := s.log
	writer := session.writer

	log.Debug("Receive client ping")
	if err := protocol.WritePingResponse(writer); err != nil {
		return err
	}
	return session.flush()
}
