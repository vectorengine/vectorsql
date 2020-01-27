[![Build Status](https://travis-ci.org/vectorengine/vectorsql.png)](https://travis-ci.org/vectorengine/vectorsql)
[![codecov.io](https://codecov.io/gh/vectorengine/vectorsql/graphs/badge.svg)](https://codecov.io/gh/vectorengine/vectorsql/branch/master)

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
```

## Client

```
$clickhouse-client --compression=0

VectorSQL :) select * from range(1,10) as r where i>3 and i<8;

SELECT *
FROM range(1, 10) AS r
WHERE (i > 3) AND (i < 8)

┌─i─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
└───┘
↘ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.)
4 rows in set. Elapsed: 0.017 sec.
```
