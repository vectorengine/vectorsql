#!/bin/bash

declare -a querys=(
"SELECT COUNT(id) FROM testdata"
"SELECT COUNT(id) FROM testdata WHERE id!=0"
"SELECT SUM(data5) FROM testdata"
"SELECT SUM(data5) AS sum, COUNT(data5) AS count, sum/count AS avg FROM testdata"
"SELECT MAX(id), MIN(id) FROM testdata"
"SELECT COUNT(data1) AS count, data1 FROM testdata GROUP BY data1 ORDER BY count DESC LIMIT 10"
"SELECT email FROM testdata WHERE email like '%20@example.com%' LIMIT 1"
"SELECT COUNT(email) FROM testdata WHERE email like '%20@example.com%'"
"SELECT data1 AS x, x - 1, x - 2, x - 3, count(data1) AS c FROM testdata GROUP BY x, x - 1, x - 2, x - 3 ORDER BY c DESC LIMIT 10"
)
for q in "${querys[@]}"
do
    r="$(clickhouse-client --compression=0 --database=benchmark --time --query="$q" 2>&1| tail -n 1)";
    echo '|' $q '|' $r's' '|'
done

