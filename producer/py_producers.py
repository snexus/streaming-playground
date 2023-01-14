from abc import ABC, abstractmethod
from typing import Callable, Protocol, Type
from uuid import uuid4

from confluent_kafka import Producer, avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from generic_event import GenericEvent, AvroParameters, JSONParameters
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient


class Event(Protocol):
    def to_dict(self) -> dict:
        ...


class PyProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        delivery_report: Callable,
    ):
        """
        Initialize the Producer class.

        Parameters:
            bootstrap_servers (str): The bootstrap servers for the Kafka cluster.
            schema_registry_url (str): The URL for the schema registry.
        """

        self.bootstap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.delivery_report = delivery_report

    @abstractmethod
    def produce(self, **kwargs):
        """
        Produce a message to a kafka topic.

        Parameters:
            **kwargs: Additional arguments to be passed to produce method.
        """

        pass


class PyAvroProducer(PyProducer):
    def __init__(
        self,
        event_type: Type[GenericEvent],
        bootstrap_servers: str,
        schema_registry: str,
        delivery_report: Callable,
    ):
        """
        Initialize the PyAvroProducer class.

        Parameters:
            key_schema_fn (str): The file path for key schema.
            value_schema_fn (str): The file path for value schema.
            bootstrap_servers (str): The bootstrap servers for the Kafka cluster.
            schema_registry (str): The URL for the schema registry.
        """

        super().__init__(bootstrap_servers, schema_registry, delivery_report)
        params = event_type.avro_ser_parameters()

        self.value_schema = avro.load(params.value_schema_fn)
        self.key_schema = avro.load(params.key_schema_fn)

        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.bootstap_servers,
                "schema.registry.url": self.schema_registry_url,
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def produce(self, topic: str, key: str, obj: Event, **kwargs):
        """
        Produce a message to a kafka topic.

        Parameters:
            topic (str): The topic to produce the message to.
            key (str): The key for the message.
            obj (Event): The object to be serialized and sent as message.
        """

        event_dict = obj.to_dict()

        self.producer.produce(
            topic=topic,
            key={"id": key},
            value=event_dict,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            on_delivery=self.delivery_report,
        )


class PyJSONProducer(PyProducer):
    def __init__(
        self,
        event_type: Type[GenericEvent],
        bootstrap_servers: str,
        schema_registry: str,
        delivery_report: Callable,
    ):
        super().__init__(bootstrap_servers, schema_registry, delivery_report)

        params = event_type.json_ser_parameters()

        with open(params.value_schema_fn) as f:
            self.key_schema = f.read()

        print(self.key_schema)
        schema_registry_conf = {"url": self.schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.string_serializer = StringSerializer("utf_8")
        self.json_serializer = JSONSerializer(
            self.key_schema, schema_registry_client, event_type.event_to_dict
        )

    def produce(self, topic: str, key: str, obj: Event, **kwargs):
        self.producer.produce(
            topic=topic,
            key=self.string_serializer(
                key, SerializationContext(topic, MessageField.KEY)
            ),
            value=self.json_serializer(
                obj, SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=self.delivery_report,
            **kwargs
        )
