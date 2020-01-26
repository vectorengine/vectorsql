// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import ()

func (s *TCPHandler) processData(session *TCPSession) error {
	return session.sendEndOfStream()
}
