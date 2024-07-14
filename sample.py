from pymongo import MongoClient
import pymongo
import urllib
import os 
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Retrieve the password from the environment variable
password = os.getenv('MONGO_DB_PASSWORD')
escaped_password = urllib.parse.quote_plus(password)
conn = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn)

#database
db = mongo_client['logistic_data']
#collection
collection = db['delivery_truck_data']



# top 5 gps provider.
query1 = [{
    '$group':{
        "_id":"$GpsProvider",
        "num_people":{"$sum":1}
    }
},
{
    "$sort":{
        "num_people":-1
    }
},
{
    "$limit":5
},
{
    '$project':{
        "GpsProviders":"$_id",
        "num_peoples":"$num_people",
        "_id": 0
        
    }
}
]


result1 = collection.aggregate(query1)
list_top5_providers = f"top 5 providers: {[result for result in result1]}"
print(list_top5_providers)


# How many trips did each delivery truck make?
query2 = [{
        '$group':{
            '_id':"$vehicle_no",
            'count':{'$sum':1}
        }
    },
    {
        '$project':{
            'truck_num':'$_id',
            'trip_count':'$count',
            '_id':0
        }
    }]

result2 = collection.aggregate(query2)
for i in result2:
    print(i)


# top 3 suppliers who provide most vehicles

query3 = [
{
  "$group": {
    "_id": "$supplierNameCode",
    "count": {
      "$sum": 1 
    }
  }
},{'$sort':{'count':-1}},{'$limit':3}
,{
    '$project':{
        "supplier_name":'$_id',
        'num_vehicles': '$count',
        '_id':0

    }
}
]
result3 = collection.aggregate(query3)
for i in result3 : print(i)

# top 5 customers along with their most shipped item

query4 = [
  {
    "$group": {
      "_id": {
        "customerNameCode": "$customerNameCode",
        "Material_Shipped": "$Material_Shipped"
      },
      "totalShipped": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "totalShipped": -1
    }
  },
  {
    "$group": {
      "_id": "$_id.customerNameCode",
      "mostShippedItem": {
        "$first": {
          "Material_Shipped": "$_id.Material_Shipped",
          "totalShipped": "$totalShipped"
        }
      }
    }
  },
  {
    "$sort": {
      "mostShippedItem.totalShipped": -1
    }
  },
  {
    "$limit": 5
  },
  {
    "$project": {
      "_id": 0,
      "customerNameCode": "$_id",
      "mostShippedItem": "$mostShippedItem.Material_Shipped",
      "totalShipped": "$mostShippedItem.totalShipped"
    }
  }
]


result4 = collection.aggregate(query4)
for i in result4 : print(i)


# Record with longest transportation distance

query5 = [ { "$sort": { "TRANSPORTATION_DISTANCE_IN_KM": -1 } }, { "$limit": 1 } ]

result5 = collection.aggregate(query5)
for result in result5: print(result)

# Most common origin and destination for delivery
query6 =   [
    {
      "$group": {
        "_id": {
          "Origin": "$Origin_Location",
          "Destination": "$Destination_Location"
        },
        "count": { "$sum": 1 }
      }
    },
    {
      "$sort": { "count": -1 }
    },
    {
      "$limit": 1
    }
  ]

result6 = collection.aggregate(query6)
for result in result6: print(result)
