#!/bin/sh

rm -r output
hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar BroadcastJoin \
simple_tables/t1.csv 0 \
simple_tables/t2.csv 0 output
