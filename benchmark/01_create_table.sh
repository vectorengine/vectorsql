#!/bin/bash

clickhouse-client --compression=0 --query="drop database benchmark"
clickhouse-client --compression=0 --query="create database benchmark"
clickhouse-client --compression=0 --query="create table benchmark.testdata(id UInt64, email String, data1 Int64, data2 Int64, data3 Int64, data4 Int64, data5 Int64) engine=Memory"

