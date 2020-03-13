// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import "sync"

type Session struct {
	mu       sync.Mutex
	id       uint64
	database string
	progress *ProgressValues
}

func NewSession() *Session {
	mgrMu.Lock()
	defer mgrMu.Unlock()
	session := &Session{
		id:       sessionID,
		database: "system",
		progress: &ProgressValues{},
	}
	sessionMgr[sessionID] = session
	sessionID++
	return session
}

func (s *Session) UpdateProgress(pv *ProgressValues) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.progress = pv
}

func (s *Session) GetProgress() *ProgressValues {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &ProgressValues{
		Cost:            s.progress.Cost,
		ReadRows:        s.progress.ReadRows,
		ReadBytes:       s.progress.ReadBytes,
		TotalRowsToRead: s.progress.TotalRowsToRead,
		WrittenRows:     s.progress.WrittenRows,
		WrittenBytes:    s.progress.WrittenBytes,
	}
}

func (s *Session) SetDatabase(db string) {
	s.database = db
}

func (s *Session) GetDatabase() string {
	return s.database
}

func (s *Session) Close() {
	mgrMu.Lock()
	defer mgrMu.Unlock()
	delete(sessionMgr, s.id)
}
