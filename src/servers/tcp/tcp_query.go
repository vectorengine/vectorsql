// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"context"
	"sync"
	"time"

	"datablocks"
	"datastreams"
	"executors"
	"optimizers"
	"planners"
	"processors"
	"servers/protocol"
	"sessions"
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
	log.Debug("TCPHandler-Query->Enter:%+v", query.Query)

	// Logical plans.
	plan, err := planners.PlanFactory(query.Query)
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}
	plan = optimizers.Optimize(plan, optimizers.DefaultOptimizers)

	// Executors.
	ectx := executors.NewExecutorContext(ctx, log, conf, xsession)
	ectx.SetProgressCallback(func(pv *sessions.ProgressValues) {
		xsession.UpdateProgress(pv)
	})
	executor, err := executors.ExecutorFactory(ectx, plan)
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}

	result, err := executor.Execute()
	if err != nil {
		log.Error("%+v", err)
		return session.sendException(err, conf.Server.CalculateTextStackTrace)
	}

	if result.In != nil {
		if err := s.processOrdinaryQuery(session, result.In); err != nil {
			return err
		}
	} else if result.Out != nil {
		if err := s.processInsertQuery(session, result.Out); err != nil {
			return err
		}
		s.state.SetExecutorResult(result)
	}
	log.Debug("%s", executor.String())
	return session.sendEndOfStream()
}

func (s *TCPHandler) processOrdinaryQuery(session *TCPSession, sink processors.IProcessor) error {
	var mu sync.Mutex
	conf := s.conf
	log := s.log
	done := make(chan struct{})
	defer close(done)

	log.Debug("TCPHandler->OrdinaryQuery->Enter")
	if sink != nil {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					mu.Lock()
					if err := session.sendProgress(); err != nil {
						mu.Unlock()
						return
					}
					mu.Unlock()
					time.Sleep(time.Millisecond * 100)
				}
			}
		}()

		for x := range sink.In().Recv() {
			switch x := x.(type) {
			case error:
				log.Error("%+v", x)
				return session.sendException(x, conf.Server.CalculateTextStackTrace)
			case *datablocks.DataBlock:
				chunks, err := x.Split(conf.Server.DefaultBlockSize)
				if err != nil {
					return err
				}

				for _, block := range chunks {
					mu.Lock()
					if err := session.sendData(block); err != nil {
						mu.Unlock()
						return err
					}
					mu.Unlock()
				}
			}
		}
	}
	log.Debug("TCPHandler->OrdinaryQuery->Return")
	return nil
}

func (s *TCPHandler) processInsertQuery(session *TCPSession, output datastreams.IDataBlockOutputStream) error {
	return session.sendData(output.SampleBlock())
}
