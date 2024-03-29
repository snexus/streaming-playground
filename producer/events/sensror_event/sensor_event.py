from ast import List
from generic_event import GenericEvent, AvroParameters, JSONParameters
import os
import numpy as np
import time

dir_path = os.path.dirname(os.path.realpath(__file__))

AVRO_SCHEMA_FOLDER = os.path.join(dir_path, "avro_schema")
JSON_SCHEMA_FOLDER = os.path.join(dir_path, "json_schema")


class SensorEvent(GenericEvent):
    
    def __init__(self, sensor_id: int, sensor_type: str, sensor_reading: float):
        """
        Initialize a SensorEvent instance.
        
        Args:
        sensor_id (int): The id of the sensor that generated the event.
        sensor_type (str): The type of sensor that generated the event.
        sensor_reading (float): The reading of the sensor at the time of the event.
        """
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.sensor_reading = sensor_reading
        self.event_timestamp =  time.time_ns() // 1000000 # In milliseconds

    @staticmethod
    def avro_ser_parameters() -> AvroParameters:
        """
        Returns AvroParameters used for serializing events.

        Returns:
        AvroParameters: The AvroParameters used for serializing events.
        """
        
        return AvroParameters(
            key_schema_fn=os.path.join(AVRO_SCHEMA_FOLDER, "KeySchema.avsc"),
            value_schema_fn=os.path.join(AVRO_SCHEMA_FOLDER, "ValueSchema.avsc"),
        )

    @staticmethod
    def json_ser_parameters() -> JSONParameters:
        return JSONParameters(
            value_schema_fn=os.path.join(JSON_SCHEMA_FOLDER, "value_schema.jsc")
        )

    def to_dict(self, **kwargs):
        """
        Returns a dict representation of a SensorEvent instance for serialization.
        Args:
            event (SensorEvent): User instance.
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.
        Returns:
            dict: Dict populated with user attributes to be serialized.
        """

        return self.event_to_dict(self, **kwargs)

    @staticmethod
    def event_to_dict(event: "SensorEvent", ctx=None, **kwargs):
        """
        Converts the given event to a dict.

        Args:
        event (SensorEvent): The event to be converted to a dict.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        kwargs: Additional keyword arguments to be passed to the dict.

        Returns:
        dict: The dict representation of the event.
        """
        
        return dict(
            sensor_id=event.sensor_id,
            sensor_type=event.sensor_type,
            sensor_reading=event.sensor_reading,
            event_timestamp = event.event_timestamp
        )


def generate_events(n_events: int) -> list:
    """
    Generates a list of SensorEvent instances. 
    
    Args: 
    n_events (int): The number of events to generate.
    
    Returns: list: List of SensorEvent instances. 
    """
    
    n_temp = n_events // 2
    n_vibration = n_events - n_temp

    sensor_ids_temp = 1 + np.random.choice(n_temp, replace=True, size=n_temp)
    sensor_ids_vibr = (
        1 + n_temp + np.random.choice(n_vibration, replace=True, size=n_vibration)
    )

    temp_readings = np.random.normal(25, 4, size=n_temp)
    vibr_readings = np.random.normal(100, 10, size=n_vibration)

    temp_events = [
        SensorEvent(sensor_id=int(i), sensor_type="temperature", sensor_reading=r)
        for i, r in zip(sensor_ids_temp, temp_readings)
    ]
    vibr_events = [
        SensorEvent(sensor_id=int(i), sensor_type="vibration", sensor_reading=r)
        for i, r in zip(sensor_ids_vibr, vibr_readings)
    ]
    all_events = temp_events + vibr_events
    np.random.shuffle(all_events)  # type: ignore
    return all_events
