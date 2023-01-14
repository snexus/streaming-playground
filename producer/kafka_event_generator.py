from time import sleep
from typing import List
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import argparse
from confluent_kafka.schema_registry import SchemaRegistryClient
import numpy as np

from py_producers import PyAvroProducer, PyJSONProducer
from events.sensror_event.sensor_event import SensorEvent, generate_events


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for SensorEvent record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def main(args):
    producer_type = {"json": PyJSONProducer, "avro": PyAvroProducer}
    if args.format not in producer_type:
        raise TypeError(
            f"Invalid serialization type {args.format}. Supported - 'json' and 'avro'"
        )

    producer = producer_type[args.format](
        event_type=SensorEvent,
        bootstrap_servers=args.bootstrap_servers,
        schema_registry=args.schema_registry,
        delivery_report=delivery_report,
    )

    print("=" * 60)
    print(f"Initialized {args.format} producer. Generating events.\n")

    events = generate_events(int(args.n_events))
    rate = float(args.rate)

    delays = 60.0 / np.random.poisson(rate, size=len(events))
    print(f"Sending {len(events)} events with average rate {rate} events / minute.")
    for i, event in enumerate(events):
        producer.produce(topic=args.topic, key=str(uuid4()), obj=event)
        sleep_sec = delays[i]
        print(f"Sending event {i} and sleeping for {sleep_sec:.2f} seconds.")
        sleep(sleep_sec)

    producer.producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Event Generator")
    parser.add_argument(
        "-b",
        dest="bootstrap_servers",
        default="localhost:9092",
        help="Bootstrap broker(s) (host[:port])",
    )
    parser.add_argument(
        "-s",
        dest="schema_registry",
        default="http://localhost:8081",
        help="Schema Registry (http(s)://host[:port]",
    )
    parser.add_argument("-t", dest="topic", required=True, help="Topic name.")
    parser.add_argument(
        "-n", dest="n_events", default=1, help="Number of events to generate."
    )
    parser.add_argument(
        "-r",
        dest="rate",
        default=100,
        help="Average rate of events per minute (Poisson)",
    )
    parser.add_argument(
        "-f", dest="format", default="avro", help="Serialization format (json, avro"
    )
    main(parser.parse_args())
