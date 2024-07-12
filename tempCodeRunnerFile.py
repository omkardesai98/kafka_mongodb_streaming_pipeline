
# key_deserializer = StringDeserializer('utf-8')
# value_deserializer = AvroDeserializer(schema_registry_client,schema_str)

# consumer = DeserializingConsumer(
#     {
#         'bootstrap.servers':kafka_config['bootstrap.servers'],
#         'sasl.mechanism':kafka_config['sasl.mechanism'],
#         'security.protocol':kafka_config['security.protocol'],
#         'sasl.username':kafka_config['sasl.username'],
#         'sasl.password': kafka_config['sasl.password'],
#         'key.deserializer':key_deserializer,
#         'value.deserializer':value_deserializer,
#         'group.id':kafka_config['group.id'],
#         'auto.offset.reset': kafka_config['auto.offset.reset']
#     }
# )

# # subscribe to the topic 
# consumer.subscribe(['logistic_data'])

# def datatype_validation_process(msg):
#     try:
#         logistic_data = LogisticData(**msg)
#         print(f"validated message: {logistic_data.model_dump(by_alias=True)}")
#     except ValidationError as e:
#         print(f"Validation error:{e}")


# def convert_data(value):
#     try:
#         value = int(value)
#         return value
#     except ValueError:
#         pass
#     try:
#         value = float(value)
#         return value
#     except ValueError:
#         pass

# # conn_string = 'mongodb+srv://omkar:omkar@123@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster'
# # client = MongoClient(conn_string)

# # select database
# # db = client['logistic_data']

# try:
#     while True:
#         msg = consumer.poll(1.0)

#         if msg is None:
#             continue
#         if msg.error():
#             print(f'consumer error: {msg.error()}')
#             continue
#         key = msg.key()
#         values = msg.value()
        
#         # converted_value = {key:convert_data(value) for key ,value in values.items()}
        
#         print(f"successfully consumed: {values} for {key} ")
            
# except KeyboardInterrupt:
#     print("consumer stopped due to keyboard interruption!")
# finally:
#     consumer.close()



