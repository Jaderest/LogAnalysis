#!/bin/bash

mvn clean package -DskipTests > /dev/null 2>&1

hdfs dfs -mkdir -p /user
hdfs dfs -mkdir -p /user/lab

task1() {
    hdfs dfs -rm -r /user/lab/task1 > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task1 /user/lab/dataset /user/lab/task1
    hdfs dfs -get /user/lab/task1
}

task2() {
    rm -r ./task2_*
    hdfs dfs -rm -r /user/lab/task2_agent > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_agent_sorted > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_hour > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_hour_sorted > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task2 /user/lab/dataset /user/lab/task2
    hdfs dfs -get /user/lab/task2_agent
    hdfs dfs -get /user/lab/task2_agent_sorted
    hdfs dfs -get /user/lab/task2_hour
    hdfs dfs -get /user/lab/task2_hour_sorted
}

task2_ver2() {
    rm -r ./task2_*
    hdfs dfs -rm -r /user/lab/task2_agent > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_agent_sorted > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_hour > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task2_hour_sorted > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task2_ver2 /user/lab/dataset /user/lab/task2
    hdfs dfs -get /user/lab/task2_agent
    hdfs dfs -get /user/lab/task2_agent_sorted
    hdfs dfs -get /user/lab/task2_hour
    hdfs dfs -get /user/lab/task2_hour_sorted
}

task3_1() {
    rm -r ./task3_1
    hdfs dfs -rm -r /user/lab/task3_1 > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task3_part1 /user/lab/dataset /user/lab/task3_1
    hdfs dfs -get /user/lab/task3_1
}

task3_2() {
    rm -r ./task3_2
    rm -r ./task3_2_sorted
    hdfs dfs -rm -r /user/lab/task3_2 > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task3_2_sorted > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task3_part2 /user/lab/dataset /user/lab/task3_2
    hdfs dfs -get /user/lab/task3_2
    hdfs dfs -get /user/lab/task3_2_sorted
}

task3_3() {
    rm -r ./task3_3*
    hdfs dfs -rm -r /user/lab/task3_3 > /dev/null 2>&1
    hdfs dfs -rm -r /user/lab/task3_3_sorted > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task3_part3 /user/lab/dataset /user/lab/task3_3 /user/lab/task2_agent_sorted/part-r-00000
    hdfs dfs -get /user/lab/task3_3
    hdfs dfs -get /user/lab/task3_3_sorted
}

task4_1() {
    hdfs dfs -rm -r /user/lab/task4_1 > /dev/null 2>&1
    hadoop jar ./target/log_analysis-1.0.jar com.hdp.task4_part1 /user/lab/dataset /user/lab/task4_1
    hdfs dfs -get /user/lab/task4_1
}


case "$1" in
    "1")
        task1
        ;;
    "2")
        task2
        ;;
    "2_ver2")
        task2_ver2
        ;;
    "3-1")
        task3_1
        ;;
    "3_1")
        task3_1
        ;;
    "3-2")
        task3_2
        ;;
    "3_2")
        task3_2
        ;;
    "3-3")
        task3_3
        ;;
    "3_3")
        task3_3
        ;;
    "4-1")
        task4_1
        ;;
    "4_1")
        task4_1
        ;;
    *)
        exit 1
        ;;
esac