// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package http

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"config"

	"base/errors"
	"base/xlog"
)

type HTTPHandler struct {
	httpServer *http.Server
	log        *xlog.Log
	conf       *config.Config
}

func NewHTTPHandler(log *xlog.Log, conf *config.Config) *HTTPHandler {
	s := &HTTPHandler{
		log:        log,
		conf:       conf,
		httpServer: &http.Server{Addr: fmt.Sprintf("%v:%v", conf.Server.ListenHost, conf.Server.HTTPPort)},
	}
	s.httpServer.Handler = s
	return s
}

func (s *HTTPHandler) Start() {
	log := s.log
	go func() {
		log.Fatal("%v", s.httpServer.ListenAndServe())
	}()
}

func (s *HTTPHandler) Stop() {
}

func (s *HTTPHandler) Address() string {
	return fmt.Sprintf(":%v", s.conf.Server.HTTPPort)
}

func (s *HTTPHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	log := s.log

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Error("%+v", errors.Errorf("%+v", x))
		}
	}()

	// extract the query SQL
	var query = req.URL.Query().Get("query")
	if query == "" && req.Method == http.MethodPost {
		bs, err := ioutil.ReadAll(req.Body)
		if err != nil {
			fmt.Fprintf(rw, "%v", err.Error())
			return
		}
		defer req.Body.Close()
		query = string(bs)
	}

	if err := s.processQuery(query, rw); err != nil {
		fmt.Fprintf(rw, "%v", err.Error())
		return
	}
}
