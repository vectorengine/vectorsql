// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package tcp

import (
	"context"

	"datablocks"
	"executors"
	"processors"

	"servers/protocol"
)

func (s *TCPHandler) processQuery(session *TCPSession) error {
	var (
		err error

		log      = s.log
		conf     = s.conf
		reader   = session.reader
		xsession = session.session
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Request.
	query, err := protocol.ReadQueryRequest(reader, session.hello.ClientRevision)
	if err != nil {
		return err
	}
	log.Debug("TCPHandler-Query->Enter:%+v", query.Query)

	sink, err := executors.ExecuteQuery(ctx, query.Query, log, conf, xsession)
	if err != nil {
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

	log.Debug("TCPHandler->OrdinaryQuery->Enter")
	if sink != nil {
		for x := range sink.In().Recv() {
			switch x := x.(type) {
			case error:
				log.Error("%+v", x)
				return session.sendException(x, conf.Server.CalculateTextStackTrace)
			case *datablocks.DataBlock:
				log.Debug("TCPHandler->OrdinaryQuery->DataBlock->Enter->Send datas: rows:%+v", x.NumRows())
				chunks := x.Split(conf.Server.DefaultBlockSize)
				for _, block := range chunks {
					if err := session.sendData(block); err != nil {
						return err
					}
				}
				log.Debug("TCPHandler->OrdinaryQuery->DataBlock->Enter->Send data done")
			}
		}
	}
	log.Debug("TCPHandler->OrdinaryQuery->Return")
	return nil
}
