#!/bin/bash

END=${END-10000000}

for i in $(seq 1 $END); do
    rand1=$(($RANDOM % 50));
    rand2=$(($RANDOM % 10000));
    rand3=$(($RANDOM % 100000));
    rand4=$(($RANDOM % 1000000));
    rand5=$(($RANDOM % 10000000));
    printf "%d\t%08d@example.com\t%d\t%d\t%d\t%d\t%d\n" $i $i $rand1 $rand2 $rand3 $rand4 $rand5;
done 
