#!/bin/sh

rm -r output
hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar joins.com.alexselzer.mrjoins.joins.MergeJoin \
merge_join_tables/t1 0 \
merge_join_tables/t2 0 output
