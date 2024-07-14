from flask import Flask, request, jsonify
from pymongo import MongoClient
import urllib
from bson.json_util import dumps
import os 
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables
load_dotenv()

# Replace the following with your MongoDB Atlas connection string
password = os.environ.get('MONGO_DB_PASSWORD')
escaped_password = urllib.parse.quote_plus(password)
conn = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn)

# Database
db = mongo_client['logistic_data']
# Collection
collection = db['delivery_truck_data']

# Endpoint to get top 5 GPS providers
@app.route('/top-gps-providers', methods=['GET'])
def get_top_gps_providers():
    try:
        query = [
            {'$group': {"_id": "$GpsProvider", "num_people": {"$sum": 1}}},
            {'$sort': {"num_people": -1}},
            {'$limit': 5},
            {'$project': {"GpsProvider": "$_id", "num_people": "$num_people", "_id": 0}}
        ]
        result = collection.aggregate(query)
        return dumps(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint to filter documents
@app.route('/documents/filter', methods=['GET'])
def filter_documents():
    try:
        query = request.args.to_dict()
        documents = collection.find(query)
        return dumps(documents), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint to aggregate data
@app.route('/documents/aggregate', methods=['POST'])
def aggregate_documents():
    try:
        pipeline = request.json.get('pipeline', [])
        results = collection.aggregate(pipeline)
        return dumps(results), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

