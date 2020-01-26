// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package errors

import (
	"bytes"
	"fmt"
	"io"
)

type Error struct {
	num   int
	msg   string
	stack *stack
	cause error
}

func (e *Error) Code() int {
	return e.num
}

func New(msg string) error {
	return &Error{
		msg:   msg,
		stack: caller(),
	}
}

func Errorf(format string, args ...interface{}) error {
	return &Error{
		msg:   fmt.Sprintf(format, args...),
		stack: caller(),
	}
}

func ErrorWithCode(num int, format string, args ...interface{}) error {
	return &Error{
		num:   num,
		msg:   fmt.Sprintf(format, args...),
		stack: caller(),
	}
}

func Wrap(err error) error {
	if xerr, ok := err.(*Error); ok {
		return xerr
	}
	return &Error{
		msg:   err.Error(),
		stack: caller(),
	}
}

func Wrapf(err error, format string, args ...interface{}) error {
	if xerr, ok := err.(*Error); ok {
		xerr.msg = fmt.Sprintf(format, args...) + ": " + xerr.msg
		xerr.cause = err
		return xerr
	}
	return &Error{
		stack: caller(),
		cause: err,
		msg:   fmt.Sprintf(format, args...) + ": " + err.Error(),
	}
}

func (e *Error) Error() string {
	buf := &bytes.Buffer{}
	buf.WriteString(e.msg)
	if e.num != 0 {
		buf.WriteString(fmt.Sprintf(" (errno %d)", e.num))
	}
	return buf.String()
}

func (e *Error) Cause() error {
	return e.cause
}

func (e *Error) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		_, _ = io.WriteString(s, e.msg)
		if e.num != 0 {
			_, _ = io.WriteString(s, fmt.Sprintf(" (errno %d)", e.num))
		}
		if s.Flag('+') {
			_, _ = io.WriteString(s, "\n"+e.stack.trace())
		}
	case 's':
		_, _ = io.WriteString(s, e.msg)
	}
}
