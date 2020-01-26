// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import ()

type Session struct {
	database string
}

func NewSession() *Session {
	return &Session{
		database: "system",
	}
}

func (s *Session) SetDatabase(db string) {
	s.database = db
}

func (s *Session) GetDatabase() string {
	return s.database
}
