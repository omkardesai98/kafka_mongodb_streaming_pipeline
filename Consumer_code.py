import json
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




def convert_to_proper_types(record):
    # Convert dates to datetime objects
    if 'BookingID_Date' in record:
        record['BookingID_Date'] = datetime.combine(datetime.strptime(record['BookingID_Date'], '%m/%d/%Y'), datetime.min.time())
    if 'actual_eta' in record:
        record['actual_eta'] = datetime.strptime(record['actual_eta'], '%m/%d/%Y %H:%M')
    if 'trip_start_date' in record:
        record['trip_start_date'] = datetime.strptime(record['trip_start_date'], '%m/%d/%Y %H:%M')

    # Convert numeric strings to floats
    if 'TRANSPORTATION_DISTANCE_IN_KM' in record:
        record['TRANSPORTATION_DISTANCE_IN_KM'] = float(record['TRANSPORTATION_DISTANCE_IN_KM'])
    if 'Curr_lat' in record:
        record['Curr_lat'] = float(record['Curr_lat'])
    if 'Curr_lon' in record:
        record['Curr_lon'] = float(record['Curr_lon'])
    # Convert duration strings to timedelta objects
    if 'Data_Ping_time' in record:
        minutes, seconds = map(float, record['Data_Ping_time'].split(':'))
        total_hours = timedelta(minutes=minutes, seconds=seconds).total_seconds()  # Convert to seconds
        record['Data_Ping_time'] = round(total_hours, 2)  # Round to 2 decimal places

    if 'Planned_ETA' in record:
        minutes, seconds = map(float, record['Planned_ETA'].split(':'))
        total_hours = timedelta(minutes=minutes, seconds=seconds).total_seconds()  # Convert to seconds
        record['Planned_ETA'] = round(total_hours, 2)  # Round to 2 decimal places

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
        
        # converted_value = {key:convert_data(value) for key ,value in values.items()}
        
        print(f"successfully consumed: {msg.value()} for {key} ")
        converted_records = convert_to_proper_types(record)

        # Dump records to JSON file
        # with open('converted_records.json', 'w') as json_file:
        #     json.dump(converted_records, json_file, default=str, indent=4)
        
        collection.insert_one(converted_records)
        print("record inserted in mongodb")
        
            
except KeyboardInterrupt:
    print("consumer stopped due to keyboard interruption!")
finally:
    consumer.close()
    mongo_client.close()


