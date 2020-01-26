// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"context"

	"datablocks"
	"executors"
	"optimizers"
	"planners"
	"processors"

	"servers/protocol"
)

func (s *TCPHandler) processQuery(session *TCPSession) error {
	var err error

	log := s.log
	conf := s.conf
	reader := session.reader
	xsession := session.session

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Request.
	query, err := protocol.ReadQueryRequest(reader, session.hello.ClientRevision)
	if err != nil {
		return err
	}
	log.Debug("Receive client query:%+v", query.Query)

	// Logical plans.
	plan, err := planners.PlanFactory(query.Query)
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}
	plan = optimizers.Optimize(plan, optimizers.DefaultOptimizers)

	// Executors.
	ectx := executors.NewExecutorContext(ctx, log, conf, xsession)
	executor, err := executors.ExecutorFactory(ectx, plan)
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}

	sink, err := executor.Execute()
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}

	if err := s.processOrdinaryQuery(session, sink); err != nil {
		return err
	}
	return session.sendEndOfStream()
}

func (s *TCPHandler) processOrdinaryQuery(session *TCPSession, sink processors.IProcessor) error {
	conf := s.conf
	log := s.log

	if sink != nil {
		for x := range sink.In().Recv() {
			switch x := x.(type) {
			case error:
				return session.sendException(x, conf.Server.CalculateTextStackTrace)
			case *datablocks.DataBlock:
				log.Debug("Response:%+v", x)
				if err := session.sendData(x); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
