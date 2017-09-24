#!/bin/bash

cd $HADOOP_PREFIX
bin/hdfs dfs -rm -r /user/root/mapreduce/topten/input
bin/hdfs dfs -rm -r /user/root/mapreduce/topten/output
bin/hdfs dfs -mkdir -p /user/root/mapreduce/topten/input
bin/hdfs dfs -put mywork/mapreduce/topten/input/users.xml /user/root/mapreduce/topten/input/users.xml
bin/hdfs dfs -ls /user/root/mapreduce/topten/input

cd mywork/mapreduce/topten

mkdir classes

javac -classpath $HADOOP_PREFIX/share/hadoop/common/hadoop-common-2.7.1.jar:$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar:$HADOOP_PREFIX/share/hadoop/common/lib/commons-cli-1.2.jar -d classes src/lab1code/TopTen.java

jar -cvf topten.jar -C classes/ .

cd $HADOOP_PREFIX

bin/hadoop jar mywork/mapreduce/topten/topten.jar lab1code.TopTen /user/root/mapreduce/topten/input /user/root/mapreduce/topten/output

bin/hdfs dfs -cat /user/root/mapreduce/topten/output/part-r-00000