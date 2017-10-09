## Data-intensive computing

### Hadoop and MapReduce

To start Hadoop shell 
and mount this repository `data-intensive-computing` 
to folder internally in the container:

```
REPO=/home/mrow4a/Projekty/data-intensive-computing
sudo docker rm -f Hadoop
sudo docker run -it -v $REPO:/usr/local/hadoop/mywork -p 8088:8088 -p 50070:50070 --name Hadoop sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash

```

To execute WordCount job:

```
/usr/local/hadoop/mywork/mapreduce/wordcount/start.sh

```

To execute TopTen job:

```
/usr/local/hadoop/mywork/mapreduce/topten/start.sh

```

### Spark and SparkSQL

```
docker pull jupyter/all-spark-notebook
REPO=/home/mrow4a/Projekty/data-intensive-computing/sparksql
docker run -it --rm -p 8888:8888 --name SparkNotebook -v $REPO:/home/jovyan/work jupyter/all-spark-notebook
```

### Flink

In this job, TaxiData is used in streaming context. We are interested in arrivals and departures in JFK Airport, and calculate for each hour, which terminal was most popular at that specific hour, in continuous time. We use taxi log timestamp as a Watermark with AssignerWithPeriodicWatermarks.

First, you need a dataset to use:

```
cd ~/WHATEVER/data-intensive-computing/flink
wget http://training.data-artisans.com/trainingData/nycTaxiRides.gz
```

In the IDE, run job `data-intensive-computing/flink/flink-java-project/src/main/java/org/apache/flink/dataintensive/AirportTrends.java` with argument  `--input /home/mrow4a/Projekty/data-intensive-computing/flink/nycTaxiRides.gz`

# GraphX

```
docker pull jupyter/all-spark-notebook
REPO=/home/mrow4a/Projekty/data-intensive-computing/graphx
docker run -it --rm -p 8888:8888 --name SparkNotebook -v $REPO:/home/jovyan/work jupyter/all-spark-notebook
```

