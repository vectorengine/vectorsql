// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"base/binary"
	"base/errors"
	"sessions"
)

func WriteProgressResponse(writer *binary.Writer, pv *sessions.ProgressValues, clientRevision uint64) error {
	// Header.
	if err := writer.Uvarint(uint64(ServerProgress)); err != nil {
		return errors.Wrapf(err, "couldn't write protocol.ServerProgress")
	}

	// Read rows.
	if err := writer.Uvarint(pv.ReadRows); err != nil {
		return errors.Wrapf(err, "couldn't write ReadRows")
	}

	// Read Bytes.
	if err := writer.Uvarint(pv.ReadBytes); err != nil {
		return errors.Wrapf(err, "couldn't write ReadBytes")
	}

	// Total Read Rows.
	if err := writer.Uvarint(pv.TotalRowsToRead); err != nil {
		return errors.Wrapf(err, "couldn't write TotalRowsToRead")
	}

	if clientRevision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
		if err := writer.Uvarint(pv.WrittenRows); err != nil {
			return errors.Wrapf(err, "couldn't write WrittenRows")
		}
		if err := writer.Uvarint(pv.WrittenBytes); err != nil {
			return errors.Wrapf(err, "couldn't write WrittenBytes")
		}
	}
	return nil
}
