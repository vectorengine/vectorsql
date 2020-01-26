// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.
package xlog

var (
	defaultName  = " "
	defaultLevel = DEBUG
)

type Options struct {
	Name  string
	Level LogLevel
}

type Option func(*Options)

func newOptions(opts ...Option) *Options {
	opt := &Options{}
	for _, o := range opts {
		o(opt)
	}

	if len(opt.Name) == 0 {
		opt.Name = defaultName
	}

	if opt.Level == 0 {
		opt.Level = defaultLevel
	}
	return opt
}

func Name(v string) Option {
	return func(o *Options) {
		o.Name = v
	}
}

func Level(v LogLevel) Option {
	return func(o *Options) {
		o.Level = v
	}
}
