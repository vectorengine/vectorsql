// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

import (
	"base/binary"
	"base/errors"
)

const (
	TCP  = 1
	HTTP = 2
)

type TCPClientInfo struct {
	OSUser             string
	ClientHostName     string
	ClientName         string
	ClientVersionMajor uint64
	ClientVersionMinor uint64
	ClientVersionPatch uint64
	ClientRevision     uint64
}

type HTTPClientInfo struct {
	HTTPMethod    uint8
	HTTPUserAgent string
}

type QueryClientInfo struct {
	QueryKind      uint64
	InitialUser    string
	InitialQueryID string
	InitialAddress string
	QuotaKey       string
	Interface      uint64
	TCPClientInfo
	HTTPClientInfo
}

type QueryProtocol struct {
	QueryID     string
	ClientInfo  *QueryClientInfo
	Stage       uint64
	Compression uint64
	Query       string
}

func ReadQueryRequest(reader *binary.Reader, clientRevision uint64) (*QueryProtocol, error) {
	var err error
	query := &QueryProtocol{}

	if query.QueryID, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read queryID")
	}

	if query.ClientInfo, err = readQueryClientInfo(reader, clientRevision); err != nil {
		return nil, err
	}

	for {
		if data, err := reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read setting data")
		} else {
			// empty string is a marker of the end of settings.
			if data == "" {
				break
			}
		}
	}

	if query.Stage, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read state")
	}

	if query.Compression, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read compression")
	}

	if query.Query, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read query string")
	}
	return query, nil
}

func readQueryClientInfo(reader *binary.Reader, clientRevision uint64) (*QueryClientInfo, error) {
	var err error
	info := &QueryClientInfo{}

	if info.QueryKind, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read queryid")
	}

	if info.QueryKind == 0 {
		return info, nil
	}

	if info.InitialUser, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read initial user")
	}

	if info.InitialQueryID, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read initial query ID")
	}

	if info.InitialAddress, err = reader.String(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read initial address string")
	}

	if info.Interface, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrapf(err, "Couldn't read interface")
	}

	switch info.Interface {
	case TCP:
		if info.OSUser, err = reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read OS user")
		}

		if info.ClientHostName, err = reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read client hostname")
		}

		if info.ClientName, err = reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read client name")
		}

		if info.ClientVersionMajor, err = reader.Uvarint(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read client version major")
		}

		if info.ClientVersionMinor, err = reader.Uvarint(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read client version minor")
		}

		if info.ClientRevision, err = reader.Uvarint(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read client revision")
		}
	case HTTP:
		if info.HTTPMethod, err = reader.UInt8(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read http method")
		}

		if info.HTTPUserAgent, err = reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read http user agent")
		}
	}

	if clientRevision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		if info.QuotaKey, err = reader.String(); err != nil {
			return nil, errors.Wrapf(err, "Couldn't read quota key")
		}
	}

	if info.Interface == TCP {
		if clientRevision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
			if info.ClientVersionPatch, err = reader.Uvarint(); err != nil {
				return nil, errors.Wrapf(err, "Couldn't read client version patch")
			}
		}
	}
	return info, nil
}
