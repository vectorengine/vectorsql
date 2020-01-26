// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"base/binary"
	"base/errors"
)

type HelloProtocol struct {
	ClientName         string
	ClientVersionMajor uint64
	ClientVersionMinor uint64
	ClientRevision     uint64
	Database           string
	User               string
	Password           string
}

func ReadHelloRequest(reader *binary.Reader) (*HelloProtocol, error) {
	var err error
	info := &HelloProtocol{}

	if info.ClientName, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read client name")
	}

	if info.ClientVersionMajor, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read client version major")
	}

	if info.ClientVersionMinor, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read client version minor")
	}

	if info.ClientRevision, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read client revision")
	}

	if info.Database, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read client database")
	}

	if info.User, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read user")
	}

	if info.Password, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "couldn't read password")
	}
	return info, nil
}

func WriteHelloResponse(writer *binary.Writer, clientRevision uint64, displayName string) error {
	if err := writer.Uvarint(uint64(ServerHello)); err != nil {
		return errors.Wrapf(err, "couldn't write packet type")
	}

	if err := writer.String(DBMSName); err != nil {
		return errors.Wrapf(err, "couldn't write dbms name")
	}

	if err := writer.Uvarint(uint64(VersionMajor)); err != nil {
		return errors.Wrapf(err, "couldn't write version major")
	}

	if err := writer.Uvarint(uint64(VersionMinor)); err != nil {
		return errors.Wrapf(err, "couldn't write version minor")
	}

	if err := writer.Uvarint(uint64(VERSION_REVISION)); err != nil {
		return errors.Wrapf(err, "couldn't write version revision")
	}

	if clientRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		//zone, _ := time.Now().Zone()
		if err := writer.String("UTC"); err != nil {
			return errors.Wrapf(err, "couldn't write timezone")
		}
	}

	if clientRevision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if err := writer.String(displayName); err != nil {
			return errors.Wrapf(err, "couldn't write display name")
		}
	}

	if clientRevision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if err := writer.Uvarint(uint64(VERSION_PATCH)); err != nil {
			return errors.Wrapf(err, "couldn't write version patch")
		}
	}
	return nil
}
