from time import sleep
from typing import List
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import argparse
from confluent_kafka.schema_registry import SchemaRegistryClient
import numpy as np



schema_str = """
{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "SensorEvent",
      "description": "A demo event",
      "type": "object",
      "properties": {

        "sensor_id": {
          "description": "Sensor id",
          "type": "number",
          "exclusiveMinimum": 0
        },
        
        "sensor_type": {
          "description": "Sensor Type",
          "type": "string"
        },
        
        "sensor_reading": {
          "description": "Sensor reading",
          "type": "number"
        }
      },
      "required": [ "sensor_id", "sensor_type", "sensor_reading" ]
    }
    """

class SensorEvent:
  def __init__(self, sensor_id: int, sensor_type: str, sensor_reading: float):
    self.sensor_id = sensor_id
    self.sensor_type = sensor_type
    self.sensor_reading = sensor_reading
    


def event_to_dict(event: SensorEvent, ctx):
  """
  Returns a dict representation of a SensorEvent instance for serialization.
  Args:
      event (SensorEvent): User instance.
      ctx (SerializationContext): Metadata pertaining to the serialization
          operation.
  Returns:
      dict: Dict populated with user attributes to be serialized.
  """
  
  return dict(sensor_id=event.sensor_id,
                sensor_type=event.sensor_type,
                sensor_reading=event.sensor_reading)


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
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    

def generate_events(n_events: int) -> List[SensorEvent]:
  
  n_temp = n_events // 2
  n_vibration = n_events - n_temp
  
  sensor_ids_temp = 1.0 + np.random.choice(n_temp, replace=True, size=n_temp)
  sensor_ids_vibr = 1.0 + n_temp + np.random.choice(n_vibration, replace=True, size=n_vibration)
  
  temp_readings = np.random.normal(25, 4, size=n_temp)
  vibr_readings = np.random.normal(100, 10, size=n_vibration)
  
  temp_events = [SensorEvent(sensor_id=i, sensor_type="temperature", sensor_reading=r) for i, r in zip(sensor_ids_temp,temp_readings)]
  vibr_events = [SensorEvent(sensor_id=i, sensor_type="vibration", sensor_reading=r) for i, r in zip(sensor_ids_vibr,vibr_readings)]
  all_events = temp_events + vibr_events
  np.random.shuffle(all_events)
  return all_events
  


def main(args):
    topic = args.topic
    
    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    string_serializer = StringSerializer('utf_8')
    
    json_serializer = JSONSerializer(schema_str, schema_registry_client, event_to_dict)
    
    producer = Producer({'bootstrap.servers': args.bootstrap_servers})
    
    #event = SensorEvent(sensor_id=1, sensor_type="temperature", sensor_reading=32.5)
    
    events = generate_events(int(args.n_events))
    rate = float(args.rate)
    
    delays = 60.0/np.random.poisson(rate,size = len(events))
    print(f"Sending {len(events)} events with average rate {rate} events / minute.")
    for i, event in enumerate(events):
      producer.produce(topic=topic, 
                              key=string_serializer(str(uuid4()), SerializationContext(topic, MessageField.KEY)),
                              value=json_serializer(event, SerializationContext(topic, MessageField.VALUE)),
                              on_delivery=delivery_report)
      sleep_sec = delays[i]
      print(f"Sending event {i} and sleeping for {sleep_sec:.2f} seconds.")
      sleep(sleep_sec)
    
    producer.flush()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="JSONSerailizer example")
    parser.add_argument('-b', dest="bootstrap_servers", default="localhost:9092",
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", default="http://localhost:8081",
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic name.")
    parser.add_argument('-n', dest="n_events", default=1,
                        help="Number of events to generate.")
    parser.add_argument('-r', dest="rate", default=100,
                        help="Average rate of events per minute (Poisson)")
    main(parser.parse_args())
    