#!/bin/bash

cd $HADOOP_PREFIX
bin/hdfs dfs -rm -r /user/root/mapreduce/wordcount/input
bin/hdfs dfs -rm -r /user/root/mapreduce/wordcount/output
bin/hdfs dfs -mkdir -p /user/root/mapreduce/wordcount/input
bin/hdfs dfs -put mywork/mapreduce/wordcount/input/file0 /user/root/mapreduce/wordcount/input
bin/hdfs dfs -put mywork/mapreduce/wordcount/input/file1 /user/root/mapreduce/wordcount/input
bin/hdfs dfs -ls /user/root/mapreduce/wordcount/input

cd mywork/mapreduce/wordcount

mkdir classes

javac -classpath $HADOOP_PREFIX/share/hadoop/common/hadoop-common-2.7.1.jar:$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar:$HADOOP_PREFIX/share/hadoop/common/lib/commons-cli-1.2.jar -d classes src/lab1code/WordCount.java

jar -cvf wordcount.jar -C classes/ .

cd $HADOOP_PREFIX

bin/hadoop jar mywork/mapreduce/wordcount/wordcount.jar lab1code.WordCount /user/root/mapreduce/wordcount/input /user/root/mapreduce/wordcount/output

bin/hdfs dfs -cat /user/root/mapreduce/wordcount/output/part-r-00000