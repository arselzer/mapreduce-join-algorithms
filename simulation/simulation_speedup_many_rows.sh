#!/bin/sh

# Run a simulation with a very large number of rows but without the broadcast join because it would fail

hdfs dfs -rm -r "*.csv"

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 200000000 -unique 0.1 -steps 10 -reducers 100 -zipf-skew 0.5 -no-broadcast-join -threads 40