// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"base/binary"
	"base/errors"
)

func WritePingResponse(writer *binary.Writer) error {
	if err := writer.Uvarint(uint64(ServerPong)); err != nil {
		return errors.Wrapf(err, "couldn't write packet type")
	}
	return nil
}
