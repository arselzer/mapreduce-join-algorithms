#!/bin/sh

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 200000 -steps 2 -unique 0.1 -reducers 4