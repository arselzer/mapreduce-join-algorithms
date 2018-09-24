#!/bin/sh

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 100000 - unique 0.1 -steps 1 -unique 0.1 -reducers 4 - threads 8