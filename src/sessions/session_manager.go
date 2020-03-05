// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import (
	"sync"
)

var (
	mgrMu      sync.RWMutex
	sessionID  uint64
	sessionMgr = make(map[uint64]*Session)
)
