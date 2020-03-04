export GO111MODULE = off
export GOPATH := $(shell pwd)

build:
	@echo "--> Building..."
	go build -v -o bin/vectorsql-server src/cmd/server.go

goyacc:
	goyacc -o src/parsers/sqlparser/sql.go src/parsers/sqlparser/sql.y

clean:
	@echo "--> Cleaning..."
	@go clean
	@rm -f bin/*


test:
	@echo "--> Testing..."
	@$(MAKE) testbase
	@$(MAKE) testconfig
	@$(MAKE) testsessions
	@$(MAKE) testexpressions
	@$(MAKE) testprocessors
	@$(MAKE) testdatastreams
	@$(MAKE) testparsers
	@$(MAKE) testplanners
	@$(MAKE) testoptimizers
	@$(MAKE) testexecutors
	@$(MAKE) testtransforms

testbase:
	go test -v -race base/xlog
	go test -v -race base/lru
	go test -v -race base/metric

testconfig:
	go test -v -race config

testsessions:
	go test -v -race sessions

testprocessors:
	go test -v -race processors

testdatastreams:
	go test -v -race datastreams

testexpressions:
	go test -v -race expressions

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
		parsers/...	\
		processors	\
		datastreams	\
		expressions	\
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
	go get -v github.com/golangci/golangci-lint/cmd/golangci-lint
	bin/golangci-lint --skip-dirs github run src/... --skip-files sql.go

fmt:
	go fmt $(pkgs)


.PHONY: build clean install fmt test coverage
