from pymongo import MongoClient
import pymongo
import urllib


password = 'omkar@123'
escaped_password = urllib.parse.quote_plus(password)
conn = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn)

#database
db = mongo_client['logistic_data']
#collection
collection = db['delivery_truck_data']

result = collection.delete_many({})

print(result)