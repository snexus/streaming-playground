from abc import ABC, abstractmethod
from collections import namedtuple

AvroParameters = namedtuple("AvroParameters", "key_schema_fn value_schema_fn")
JSONParameters = namedtuple("JSONParameters", "value_schema_fn")


class GenericEvent:
    def __init__(self):
        pass

    @staticmethod
    @abstractmethod
    def avro_ser_parameters() -> AvroParameters:
        pass

    @staticmethod
    @abstractmethod
    def json_ser_parameters() -> JSONParameters:
        pass

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def event_to_dict(event: "GenericEvent", **kwargs) -> dict:
        pass
