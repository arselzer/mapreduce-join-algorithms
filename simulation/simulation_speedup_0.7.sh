#!/bin/sh

hdfs dfs -rm -r "*.csv"

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 3000000 -increment 3000000 -steps 14 -unique 0.1 -reducers 100 -zipf-skew 0.7 -threads 40 -out results_speedup_0.7.csv
