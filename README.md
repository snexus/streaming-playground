# streaming-playground

Additional links
* https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_producer.py

# Prerequisites

## Install Confluent Kafka

Follow https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html#install-cp-using-zip-and-tar-archives

## Install Apache Spark

https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/

1) Edit .zshrc

```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
```

2) Start master and worker processes

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://ubuntu:7077
```

3) When finished, shutdown the master and slave
```bash
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```

## Starting Kafka and Schema Registry

Run in separate terminals, from confluent folder: 

1) Start ZooKeeper

```bash
bin/zookeeper-server-start ./etc/kafka/zookeeper.properties
```
2) Start Kafka.

```bash
bin/kafka-server-start ./etc/kafka/server.properties
```

3) Start Schema Registry
```bash
bin/schema-registry-start ./etc/schema-registry/schema-registry.properties
```

## Create test topic

```bash
cd producer
python ./kafka_create_topic.py -t  sensor_events
```



# Generating events

This example sends 15 events with an average rate of 100 events / minute. Rate's distribution is Poisson.

```bash
cd producer
python ./kafka_event_generator.py -t  sensor_events -n 15 -r 100
```

# Showing events

Using Kafka's console consumer:

```bash
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic sensor_events --from-beginning
```

# Showing Schemas

```bash
curl -X GET http://localhost:8081/subjects
curl -X DELETE http://localhost:8081/subjects/sensor_events_avro-value


```

# Processing events using Spark Structural Streaming


## Consuming and parsing JSON messages

```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 ./spark_stream_kafka_json.py
```

## Consuming and parsing AVRO messages
```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 ./spark_stream_kafka_avro.py

```