from pymongo import MongoClient

# client = mongo_client('localhost',27017)

# connection string

conn_string = "mongodb+srv://omkar:omkar%40123@mongodb-cluster.mqazenv.mongodb.net/"

client = MongoClient(conn_string)

db = client['aggre']

collection = db['users']

# find_result = collection.count_documents()

# print(find_result)

print(15//10)
