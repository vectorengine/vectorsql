// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package mocks

import (
	"context"
	"sync"

	"config"
	"databases"
	"sessions"

	"base/xlog"
)

var once sync.Once

type Mock struct {
	Log     *xlog.Log
	Conf    *config.Config
	Session *sessions.Session
	Ctx     context.Context
	Cancel  context.CancelFunc
}

func NewMock() (*Mock, func()) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	conf := config.DefaultConfig()
	session := sessions.NewSession()
	ctx, cancel := context.WithCancel(context.Background())

	once.Do(func() {
		if err := databases.Load(log, conf); err != nil {
			log.Panic("%+v", err)
		}
	})

	mock := &Mock{
		Log:     log,
		Conf:    conf,
		Session: session,
		Ctx:     ctx,
		Cancel:  cancel,
	}
	return mock, func() {
		cancel()
	}
}
