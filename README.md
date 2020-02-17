
[![Github Actions Status](https://github.com/vectorengine/vectorsql/workflows/VectorSQL%20CI/badge.svg)](https://github.com/vectorengine/vectorsql/actions?query=workflow%3A%22VectorSQL+CI%22)
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
VectorSQL :) select count(server) as count, sum(response_time) as resp_time, (resp_time/count) as load_avg, path,server from logmock(rows->15) where status=200 group by server,path having resp_time>5 order by server asc, load_avg desc;

SELECT 
    count(server) AS count, 
    sum(response_time) AS resp_time, 
    resp_time / count AS load_avg, 
    path, 
    server
FROM logmock(rows -> 15)
WHERE status = 200
GROUP BY 
    server, 
    path
HAVING resp_time > 5
ORDER BY 
    server ASC, 
    load_avg DESC

┌─count─┬─resp_time─┬───────────load_avg─┬─path───┬─server──────┐
│     5 │        57 │               11.4 │ /index │ 192.168.0.1 │
│     1 │        10 │                 10 │ /login │ 192.168.0.1 │
│     3 │        34 │ 11.333333333333334 │ /index │ 192.168.0.2 │
│     1 │        10 │                 10 │ /login │ 192.168.0.2 │
└───────┴───────────┴────────────────────┴────────┴─────────────┘
↓ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) 
4 rows in set. Elapsed: 0.004 sec. 
```

* http-client

```
curl -XPOST http://127.0.0.1:8123 -d "select count(server) as count, sum(response_time) as resp_time, (resp_time/count) as load_avg, path,server from logmock(rows->15) where status=200 group by server,path having resp_time>5 order by server asc, load_avg desc"
5	57	11.4	/index	192.168.0.1
1	10	10	/login	192.168.0.1
3	34	11.333333333333334	/index	192.168.0.2
1	10	10	/login	192.168.0.2
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

