#!/bin/sh

# Run a simulation with a very large number of rows but no broadcast join because it would fail

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 200000000 -steps 10 -unique-values 20000000 -reducers 100 -zipf-skew 0.5 -no-broadcast-join