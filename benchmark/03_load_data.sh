#!/bin/bash

cat data.tsv | clickhouse-client --compression=0 --database=benchmark --query="insert into testdata FORMAT TabSeparated"

