import pandas as pd
from  pymongo import MongoClient
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config = {'bootstrap.servers':'pkc-7prvp.centralindia.azure.confluent.cloud:9092',
                'sasl.mechanism':'PLAIN',
                'security.protocol':'SASL_SSL',
                'sasl.username':'TANCGCBCI57W6SEY',
                'sasl.password':'85OObb0VwOUW6SoYzYvEv75y1gJWjIFstRAiw0r2NQ664WQja68elaTk3Dd0KGNK',
                'group.id':'group1',
                'auto.offset.reset':'latest'
                }

schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info':'AU3FN3SZ6AMXZI36:gjXznU3NALcBAvo9KCrBdDBMGrf3QYChcdrxFNkXKe52ndrSH4YhgXSlnPZf2eAW'
})

subject_name = 'logistic_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


key_deserializer = StringDeserializer('utf-8')
value_deserializer = AvroDeserializer(schema_registry_client,schema_str)

consumer = DeserializingConsumer(
    {
        'bootstrap.servers':kafka_config['bootstrap.servers'],
        'security.protocol':kafka_config['security.protocol'],
        'sasl.mechanism':kafka_config['sasl.mechanism'],
        'sasl.username':kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.deserializer':key_deserializer,
        'value.deserializer':value_deserializer,
        'group.id':kafka_config['group.id'],
        'auto.offset.reset': kafka_config['auto.offset.reset']
    }
)

# subscribe to the topic 
consumer.subscribe(['logistic_data'])



def convert_data(value):
    try:
        value = int(value)
        return value
    except ValueError:
        pass
    try:
        value = float(value)
        return value
    except ValueError:
        pass


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'consumer error: {msg.error()}')
            continue
        key = msg.key()
        # values = msg.value()
        
        # converted_value = {key:convert_data(value) for key ,value in values.items()}
        
        print(f"successfully consumed: {msg.value()} for {key} ")
            
except KeyboardInterrupt:
    print("consumer stopped due to keyboard interruption!")
finally:
    consumer.close()



