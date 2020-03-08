// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"servers/protocol"
)

func (s *TCPHandler) processData(session *TCPSession) error {
	block, err := protocol.ReadDataRequest(session.reader)
	if err != nil {
		return err
	}
	if block != nil {
		if !s.state.Empty() {
			if err := s.state.result.Out.Write(block); err != nil {
				return err
			}
			s.state.Reset()
		}
	}
	return nil
}
