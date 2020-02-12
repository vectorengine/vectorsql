
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

* clickhouse-client

```
$clickhouse-client --compression=0
VectorSQL :) select c1, c2, (c1+c2) as c12, c3 from randtable(rows->50, c1->'UInt32', c2->'UInt32', c3->'String') where c12>10 and c12<20 order by c12 desc, c3 asc;

SELECT
    c1,
    c2,
    c1 + c2 AS c12,
    c3
FROM randtable(rows -> 50, c1 -> 'UInt32', c2 -> 'UInt32', c3 -> 'String')
WHERE (c12 > 10) AND (c12 < 20)
ORDER BY
    c12 DESC,
    c3 ASC

┌─c1─┬─c2─┬─c12─┬─c3────────┐
│  5 │ 14 │  19 │ string-23 │
│  0 │ 19 │  19 │ string-34 │
│  0 │ 17 │  17 │ string-46 │
│  3 │ 13 │  16 │ string-4  │
│  3 │  9 │  12 │ string-44 │
└────┴────┴─────┴───────────┘
↑ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.)
5 rows in set. Elapsed: 0.006 sec.
```

* http-client

```
curl -XPOST http://127.0.0.1:8123 -d "select c1, c2, (c1+c2) as c12, c3 from randtable(rows->50, c1->'UInt32', c2->'UInt32', c3->'String') where c12>10 and c12<20 order by c12 desc, c3 asc"
14	5	19	string-33
1	15	16	string-32
4	12	16	string-7
11	4	15	string-35
10	1	11	string-31
```

## Metrics

http://localhost:8080/debug/metrics