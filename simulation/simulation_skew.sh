#!/bin/sh

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.1 -threads 40 -out results_skew_0,1.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.2 -threads 40 -no-header -out results_skew_02.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.3 -threads 40 -no-header -out results_skew_0,3.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.4 -threads 40 -no-header -out results_skew_0,4.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.5 -threads 40 -no-header -out results_skew_0,5.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.6 -threads 40 -no-header -out results_skew_0,6.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.7 -threads 40 -no-header -out results_skew_0,7.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.8 -threads 40 -no-header -out results_skew_0,8.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 0.9 -threads 40 -no-header -out results_skew_0,9.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.0 -threads 40 -no-header -out results_skew_1,0.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.1 -threads 40 -no-header -out results_skew_1,1.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.2 -threads 40 -no-header -out results_skew_1,2.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.3 -threads 40 -no-header -out results_skew_1,3.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.4 -threads 40 -no-header -out results_skew_1,4.csv

hadoop jar ../target/mapreduce-join-comparison-1.0-SNAPSHOT.jar com.alexselzer.mrjoins.JoinSimulation \
    -rows 6000000 -steps 1 -unique 0.1 -reducers 100 -zipf-skew 1.5 -threads 40 -no-header -out results_skew_1,5.csv