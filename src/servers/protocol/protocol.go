// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package protocol

const (
	// Name, version, revision
	ServerHello = iota

	// A block of data (compressed or not)
	ServerData

	// The exception during query execution
	ServerException

	// Query execution progress: rows read, bytes read
	ServerProgress

	// Ping response
	ServerPong

	// All packets were transmitted
	ServerEndOfStream

	// Packet with profiling info
	ServerProfileInfo

	// A block with totals (compressed or not)
	ServerTotals

	// A block with minimums and maximums (compressed or not)
	ServerExtremes

	// A response to TablesStatus request.
	ServerTablesStatusResponse

	// System logs of the query execution
	ServerLog

	// Columns' description for default values calculation
	ServerTableColumns
)

const (
	// Name, version, revision, default DB
	ClientHello = iota

	// Query id, query settings, stage up to which the query must be executed,
	// whether the compression must be used,
	// query text (without data for INSERTs).
	ClientQuery

	// A block of data (compressed or not)
	ClientData

	// Cancel the query execution
	ClientCancel

	// Check that connection to the server is alive
	ClientPing

	// Check status of tables on the server
	ClientTablesStatusRequest

	// Keep the connection alive
	ClientKeepAlive

	// A block of data (compressed or not)
	ClientScalar
)

const (
	VERSION_REVISION                                = 54428
	DBMS_MIN_REVISION_WITH_CLIENT_INFO              = 54032
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
	DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME      = 54372
	DBMS_MIN_REVISION_WITH_VERSION_PATCH            = 54401
)

func ClientPacketType(typ uint64) string {
	switch typ {
	case ClientHello:
		return "ClientHello"
	case ClientQuery:
		return "ClientQuery"
	case ClientData:
		return "ClientData"
	case ClientCancel:
		return "ClientCancel"
	case ClientPing:
		return "ClientPing"
	case ClientTablesStatusRequest:
		return "ClientTablesStatusRequest"
	case ClientKeepAlive:
		return "ClientKeepAlive"
	case ClientScalar:
		return "ClientScalar"
	}
	return "Unknow"
}
