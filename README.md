
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
VectorSQL :) select sum(c1) as c1_sum, count(c1) as c1_count, c1_sum/c1_count as c1_avg, c2, c3 from randtable(rows->1000, c1->'UInt32', c2->'UInt32', c3->'String') where c1>80 and (c1+c2)<500 group by c3 order by c1_count desc, c3 asc limit 10;

SELECT 
    sum(c1) AS c1_sum, 
    count(c1) AS c1_count, 
    c1_sum / c1_count AS c1_avg, 
    c2, 
    c3
FROM randtable(rows -> 1000, c1 -> 'UInt32', c2 -> 'UInt32', c3 -> 'String')
WHERE (c1 > 80) AND ((c1 + c2) < 500)
GROUP BY c3
ORDER BY 
    c1_count DESC, 
    c3 ASC
LIMIT 10

┌─c1_sum─┬─c1_count─┬─c1_avg─┬──c2─┬─c3─────────┐
│    660 │        3 │    220 │ 326 │ string-363 │
│    295 │        1 │    295 │ 175 │ string-1   │
│    110 │        1 │    110 │ 302 │ string-100 │
│    165 │        1 │    165 │ 273 │ string-112 │
│    105 │        1 │    105 │ 241 │ string-125 │
│    132 │        1 │    132 │ 252 │ string-126 │
│    283 │        1 │    283 │  60 │ string-131 │
│    207 │        1 │    207 │ 194 │ string-143 │
│    116 │        1 │    116 │ 251 │ string-144 │
│    125 │        1 │    125 │  67 │ string-15  │
└────────┴──────────┴────────┴─────┴────────────┘
← Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) 
10 rows in set. Elapsed: 0.006 sec. 
```

* http-client

```
 curl -XPOST http://127.0.0.1:8123 -d "select sum(c1) as c1_sum, count(c1) as c1_count, c1_sum/c1_count as c1_avg, c2, c3 from randtable(rows->1000, c1->'UInt32', c2->'UInt32', c3->'String') where c1>80 and (c1+c2)<500 group by c3 order by c1_count desc, c3 asc limit 5"
590	2	295	90	string-431
243	2	121.5	346	string-433
239	1	239	255	string-13
108	1	108	318	string-15
187	1	187	78	string-173
```

## Metrics

http://localhost:8080/debug/metrics