// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package http

import (
	"context"
	"io"

	"datablocks"
	"executors"
	"formats"
	"processors"
	"sessions"
)

func (s *HTTPHandler) processQuery(query string, rw io.Writer) (err error) {
	var (
		log     = s.log
		conf    = s.conf
		session = sessions.NewSession()
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Debug("HTTPHandler-Query->Enter:%+v", query)
	sink, err := executors.ExecuteQuery(ctx, query, log, conf, session)
	if err != nil {
		return
	}

	if err = s.processOrdinaryQuery(rw, session, sink); err != nil {
		return
	}
	return nil
}

func (s *HTTPHandler) processOrdinaryQuery(rw io.Writer, session *sessions.Session, sink processors.IProcessor) error {
	log := s.log

	log.Debug("HTTPHandler->OrdinaryQuery->Enter")
	if sink != nil {
		for x := range sink.In().Recv() {
			switch x := x.(type) {
			case error:
				log.Error("%+v", x)
				return x
			case *datablocks.DataBlock:
				log.Debug("HTTPHandler->OrdinaryQuery->DataBlock: rows:%+v", x.NumRows())
				if err := s.sendData(rw, x); err != nil {
					return err
				}
			}
		}
	}
	log.Debug("HTTPHandler->OrdinaryQuery->Return")
	return nil
}

func (s *HTTPHandler) sendData(writer io.Writer, block *datablocks.DataBlock) error {
	output := formats.FactoryGetOutput("TSV")(writer)
	if err := output.Write(block); err != nil {
		return err
	}
	return nil
}
