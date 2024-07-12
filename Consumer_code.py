from datetime import datetime,timedelta
import urllib.parse
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

subject_name = 'logistic_data1-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


key_deserializer = StringDeserializer('utf-8')
value_deserializer = AvroDeserializer(schema_registry_client,schema_str)


# mongodb connection
password = 'omkar@123'
escaped_password = urllib.parse.quote_plus(password)
conn = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn)

#database
db = mongo_client['logistic_data']
#collection
collection = db['delivery_truck_data']

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
consumer.subscribe(['logistic_data1'])



def parse_date(date_str):
    for fmt in ('%m/%d/%Y %H:%M', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None

def convert_to_proper_types(record):
    # Convert dates to datetime objects
    if record.get('BookingID_Date'):
        record['BookingID_Date'] = parse_date(record['BookingID_Date'])
    if record.get('actual_eta'):
        record['actual_eta'] = parse_date(record['actual_eta'])
    if record.get('trip_start_date'):
        record['trip_start_date'] = parse_date(record['trip_start_date'])
    if record.get('data_ping_time'):
        record['data_ping_time'] = parse_date(record['data_ping_time'])
    if record.get('planned_eta'):
        record['planned_eta'] = parse_date(record['planned_eta'])

    # Convert numeric strings to floats
    if record.get('TRANSPORTATION_DISTANCE_IN_KM'):
        record['TRANSPORTATION_DISTANCE_IN_KM'] = float(record['TRANSPORTATION_DISTANCE_IN_KM']) if record['TRANSPORTATION_DISTANCE_IN_KM'] else 0.0
    if record.get('Curr_lat'):
        record['Curr_lat'] = float(record['Curr_lat']) if record['Curr_lat'] else 0.0
    if record.get('Curr_lon'):
        record['Curr_lon'] = float(record['Curr_lon']) if record['Curr_lon'] else 0.0

    return record


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'consumer error: {msg.error()}')
            continue
        key = msg.key()
        record = msg.value()
        
        print(f"successfully consumed: {msg.value()} for {key} ")
        converted_records = convert_to_proper_types(record)

        
        collection.insert_one(converted_records)
        print("record inserted in mongodb")
        
            
except KeyboardInterrupt:
    print("consumer stopped due to keyboard interruption!")
finally:
    consumer.close()
    mongo_client.close()


