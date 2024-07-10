import json
import pandas as pd

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer



kafka_config = {
    'bootstrap.servers': 'pkc-7prvp.centralindia.azure.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'TANCGCBCI57W6SEY',
    'sasl.password': '85OObb0VwOUW6SoYzYvEv75y1gJWjIFstRAiw0r2NQ664WQja68elaTk3Dd0KGNK'

}

scheme_registry_client = SchemaRegistryClient({
    'url':'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('AU3FN3SZ6AMXZI36','gjXznU3NALcBAvo9KCrBdDBMGrf3QYChcdrxFNkXKe52ndrSH4YhgXSlnPZf2eAW')
})

# assigning topic or subject name 
# sub

# schema_str = scheme_registry_client.get_latest_version()



data = pd.read_csv('E:\GrowDataskills\MongoDB\Class_Assignment\delivery_trip_truck_data.csv')

for index,value in data.iterrows():
    key = index
    value = value.to_dict()
    # print(key)
    value_result = json.dumps(value,indent=4)
    print(value_result)
    break