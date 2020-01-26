// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package errors

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

type stack struct {
	stacks []uintptr
}

func (s *stack) trace() string {
	buf := bytes.Buffer{}
	for _, stack := range s.stacks {
		fn := runtime.FuncForPC(stack)
		file, line := fn.FileLine(stack)
		buf.WriteString(fmt.Sprintf("%s:%d - %s\n", file, line, fnname(fn.Name())))
	}
	return fmt.Sprintf("%s", buf.Bytes())
}

func caller() *stack {
	stacks := make([]uintptr, 32)
	n := runtime.Callers(3, stacks[:])
	return &stack{
		stacks: stacks[:n],
	}
}

func fnname(name string) string {
	i := strings.LastIndex(name, "/")
	name = name[i+1:]
	i = strings.Index(name, ".")
	return name[i+1:]
}
