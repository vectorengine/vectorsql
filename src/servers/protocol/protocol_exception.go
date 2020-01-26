// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"fmt"

	"base/binary"
	"base/errors"
)

type ExceptionProtocol struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	Error      error
}

func WriteExceptionResponse(writer *binary.Writer, err error, withStackTrace bool) error {
	// Header.
	if err := writer.Uvarint(uint64(ServerException)); err != nil {
		return errors.Wrapf(err, "couldn't write protocol.ServerException")
	}

	var code uint32
	if xerr, ok := err.(*errors.Error); ok {
		code = uint32(xerr.Code())
	}

	// Code.
	if err := writer.UInt32(code); err != nil {
		return errors.Wrapf(err, "couldn't write code")
	}

	// Name.
	if err := writer.String(""); err != nil {
		return errors.Wrapf(err, "couldn't write name")
	}

	// Message.
	if err := writer.String(err.Error()); err != nil {
		return errors.Wrapf(err, "couldn't write message")
	}

	// StackTrace.
	var stackTrace string
	if withStackTrace {
		stackTrace = fmt.Sprintf("%+v", err)
	} else {
		stackTrace = fmt.Sprintf("%v", err)
	}
	if err := writer.String(stackTrace); err != nil {
		return errors.Wrapf(err, "couldn't write stack trace:%s", stackTrace)
	}

	// Nested.
	if err := writer.Bool(false); err != nil {
		return errors.Wrapf(err, "couldn't write nested")
	}
	return nil
}
