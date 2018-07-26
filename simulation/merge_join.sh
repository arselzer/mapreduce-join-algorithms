#!/bin/sh

rm -r output
hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar MergeJoin \
merge_join_tables/t1.txt 0 \
merge_join_tables/t2.txt 0 output
