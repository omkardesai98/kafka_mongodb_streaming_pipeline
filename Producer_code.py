import math
import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} partition [{msg.partition()}] at offset {msg.offset()}")
  

kafka_config = {
    'bootstrap.servers': 'pkc-7prvp.centralindia.azure.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'TANCGCBCI57W6SEY',
    'sasl.password': '85OObb0VwOUW6SoYzYvEv75y1gJWjIFstRAiw0r2NQ664WQja68elaTk3Dd0KGNK'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info': 'AU3FN3SZ6AMXZI36:gjXznU3NALcBAvo9KCrBdDBMGrf3QYChcdrxFNkXKe52ndrSH4YhgXSlnPZf2eAW'
})

subject_name = 'logistic_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema_str)

producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanism': kafka_config['sasl.mechanism'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': value_serializer
})

def convert_to_str(value):
    converted_value = {}
    for key, val in value.items():
        if isinstance(val, float) and pd.isna(val):  # Handle NaN values
            converted_value[key] = None
        else:
            converted_value[key] = str(val) if not pd.isna(val) else None  # Convert to string, handle NaNs
    return converted_value

try:
    logistic_data = pd.read_csv('delivery_trip_truck_data.csv')
    
    for index, value in logistic_data.iterrows():
        value = value.to_dict()
        converted_value = convert_to_str(value)
        producer.produce(topic='logistic_data', key=str(index), value=converted_value, on_delivery=delivery_report)
        producer.flush()
        time.sleep(2)
    
    print("All data published successfully")
except KeyboardInterrupt:
    print("Shutting down producer due to keyboard interruption!")
    producer.flush()
except Exception as e:
    print(f"An error occurred: {e}")
    producer.flush()







