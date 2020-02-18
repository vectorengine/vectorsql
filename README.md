
[![Github Actions Status](https://github.com/vectorengine/vectorsql/workflows/VectorSQL%20Build/badge.svg)](https://github.com/vectorengine/vectorsql/actions?query=workflow%3A%22VectorSQL+Build%22)
[![Github Actions Status](https://github.com/vectorengine/vectorsql/workflows/VectorSQL%20Test/badge.svg)](https://github.com/vectorengine/vectorsql/actions?query=workflow%3A%22VectorSQL+Test%22)
[![Github Actions Status](https://github.com/vectorengine/vectorsql/workflows/VectorSQL%20Coverage/badge.svg)](https://github.com/vectorengine/vectorsql/actions?query=workflow%3A%22VectorSQL+Coverage%22)
[![codecov.io](https://codecov.io/gh/vectorengine/vectorsql/branch/master/graph/badge.svg)](https://codecov.io/gh/vectorengine/vectorsql/branch/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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
VectorSQL :) SELECT SUM(IF(status!=200, 1, 0)) AS errors, SUM(IF(status=200, 1, 0)) as success, (errors/COUNT(server)) AS error_rate,(success/COUNT(server)) as success_rate, (SUM(response_time)/COUNT(server)) AS load_avg, MIN(response_time), MAX(response_time), path, server FROM logmock(rows->15) GROUP BY server, path HAVING errors>0 ORDER BY server ASC, load_avg DESC;

SELECT 
    SUM(IF(status != 200, 1, 0)) AS errors, 
    SUM(IF(status = 200, 1, 0)) AS success, 
    errors / COUNT(server) AS error_rate, 
    success / COUNT(server) AS success_rate, 
    SUM(response_time) / COUNT(server) AS load_avg, 
    MIN(response_time), 
    MAX(response_time), 
    path, 
    server
FROM logmock(rows -> 15)
GROUP BY 
    server, 
    path
HAVING errors > 0
ORDER BY 
    server ASC, 
    load_avg DESC

┌─errors─┬─success─┬─error_rate─┬─success_rate─┬─load_avg─┬─MIN(response_time)─┬─MAX(response_time)─┬─path───┬─server──────┐
│      2 │       1 │     0.6667 │       0.3333 │       12 │                 10 │                 13 │ /login │ 192.168.0.1 │
│      1 │       5 │     0.1667 │       0.8333 │  11.1667 │                 10 │                 12 │ /index │ 192.168.0.1 │
│      1 │       3 │       0.25 │         0.75 │    11.25 │                 10 │                 14 │ /index │ 192.168.0.2 │
│      1 │       1 │        0.5 │          0.5 │       11 │                 10 │                 12 │ /login │ 192.168.0.2 │
└────────┴─────────┴────────────┴──────────────┴──────────┴────────────────────┴────────────────────┴────────┴─────────────┘
↓ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) 
4 rows in set. Elapsed: 0.005 sec. 
```

* http-client

```
curl -XPOST http://127.0.0.1:8123 -d "SELECT SUM(IF(status!=200, 1, 0)) AS errors, SUM(IF(status=200, 1, 0)) as success, (errors/COUNT(server)) AS error_rate,(success/COUNT(server)) as success_rate, (SUM(response_time)/COUNT(server)) AS load_avg, MIN(response_time), MAX(response_time), path, server FROM logmock(rows->15) GROUP BY server, path HAVING errors>0 ORDER BY server ASC, load_avg DESC"
2	1	0.6667	0.3333	12.0000	10	13	/login	192.168.0.1
1	5	0.1667	0.8333	11.1667	10	12	/index	192.168.0.1
1	3	0.2500	0.7500	11.2500	10	14	/index	192.168.0.2
1	1	0.5000	0.5000	11.0000	10	12	/login	192.168.0.2
```

## Query Language Features

|Query language                 |Current version|Future versions|Example                   |
|-------------------------------|---------------|---------------|--------------------------|
|Scans by Value                 |+              |+              |SELECT a,b                |
|Scans by Expression            |+              |+              |SELECT IF(a>2,a,b),SUM(a) |
|Filter by Value                |+              |+              |WHERE a>10                |
|Filter by Expression           |+              |+              |WHERE a>(b+10)            |
|Group-Aggregate by Value       |+              |+              |GROUP BY a                |
|Group-Aggregate by Expression  |+              |+              |GROUP BY (a+1)            |
|Group-Having by Value          |+              |+              |HAVING count_a>2          |
|Group-Having by Expression     |+              |+              |HAVING (count_a+1)>2      |
|Order by Value                 |+              |+              |ORDER BY a desc           |
|Order by Expression            |+              |+              |ORDER BY (a+b)            |
|Window Functions               |-              |+              |                          |
|Join                           |-              |+              |                          |


## Metrics

http://localhost:8080/debug/metrics

