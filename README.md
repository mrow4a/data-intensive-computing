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