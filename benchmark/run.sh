#!/bin/bash

echo '01_create_table.sh'
./01_create_table.sh

echo '02_generate_data.sh'
COUNT=10000000 ./02_generate_data.sh

echo '03_load_data.sh'
./03_load_data.sh

echo '04_run_bench.sh'
./04_run_bench.sh




