# streaming-playground

## Install Kafka

Follow https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html#install-cp-using-zip-and-tar-archives


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
 python ./producer/kafka_create_topic.py -t  sensor_events
```