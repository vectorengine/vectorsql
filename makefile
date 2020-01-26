export GOPATH := $(shell pwd)

build:
	@echo "--> Building..."
	go build -v -o bin/vectorsql-server src/cmd/server.go

goyacc:
	goyacc -o src/parser/sql.go src/parser/sql.y

clean:
	@echo "--> Cleaning..."
	@go clean
	@rm -f bin/*

fmt:
	go fmt ./...
	go vet ./...

test:
	@echo "--> Testing..."
	@$(MAKE) testbase
	@$(MAKE) testconfig
	@$(MAKE) testsessions
	@$(MAKE) testfunctions
	@$(MAKE) testprocessors
	@$(MAKE) testdatastreams
	@$(MAKE) testparsers
	@$(MAKE) testplanners
	@$(MAKE) testoptimizers
	@$(MAKE) testexecutors
	@$(MAKE) testtransforms

testbase:
	go test -v -race base/xlog

testconfig:
	go test -v -race config

testsessions:
	go test -v -race sessions

testprocessors:
	go test -v -race processors

testdatastreams:
	go test -v -race datastreams

testfunctions:
	go test -v -race functions

testparsers:
	go test -v -race parsers/...

testplanners:
	go test -v -race planners

testoptimizers:
	go test -v -race optimizers

testexecutors:
	go test -v -race executors

testtransforms:
	go test -v -race transforms


pkgs =	config		\
		sessions	\
		processors	\
		datastreams	\
		functions	\
		planners	\
		optimizers	\
		executors	\
		transforms

coverage:
	go build -v -o bin/gotestcover \
	src/vendor/github.com/pierrre/gotestcover/*.go;
	bin/gotestcover -coverprofile=coverage.out -v $(pkgs)
	go tool cover -html=coverage.out

check:
	golangci-lint run src/...

.PHONY: build clean install fmt test coverage
