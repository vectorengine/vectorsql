[![Build Status](https://api.travis-ci.org/vectorengine/vectorsql.svg?branch=master)](https://travis-ci.org/vectorengine/vectorsql)
[![codecov.io](https://codecov.io/gh/vectorengine/vectorsql/branch/master/graph/badge.svg)](https://codecov.io/gh/vectorengine/vectorsql/branch/master)

# VectorSQL

VectorSQL is a free analytics DBMS for IoT & Big Data, compatible with ClickHouse.

## Features

* **High Performance**
* **High Scalability**
* **High Reliability**

## Server

```
$git clone https://github.com/vectorengine/vectorsql
$cd vectorsql
$make build
$./bin/vectorsql-server -c conf/vectorsql-default.toml
	
 2020/01/27 19:02:39.245654    	 [DEBUG] 	Database->Attach Table:system.tables, engine:SYSTEM_TABLES <attachTable@database_system.go:116>
 2020/01/27 19:02:39.245670    	 [DEBUG] 	Database->Attach Table:system.databases, engine:SYSTEM_DATABASES <attachTable@database_system.go:116>
 2020/01/27 19:02:39.245680    	 [INFO] 	Database->Load Database:system <loadSystemDatabases@databases.go:110>
 2020/01/27 19:02:39.245794    	 [INFO] 	Listening for connections with native protocol (tcp)::9000 <Start@server.go:33>
 2020/01/27 19:02:39.245806    	 [INFO] 	Servers start... <main@server.go:62>
```

## Client

```
$clickhouse-client --compression=0

VectorSQL :) select * from randtable(rows->10, c1->'UInt32', c2->'String') where c1>3 and c1<8 order by c1 asc, c2 desc;

SELECT *
FROM randtable(rows -> 10, c1 -> 'UInt32', c2 -> 'String')
WHERE (c1 > 3) AND (c1 < 8)
ORDER BY
    c1 ASC,
    c2 DESC

┌─c1─┬─c2───────┐
│  5 │ string-6 │
│  6 │ string-8 │
│  6 │ string-0 │
│  7 │ string-4 │
│  7 │ string-3 │
└────┴──────────┘
↖ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.)
5 rows in set. Elapsed: 0.007 sec.
```
