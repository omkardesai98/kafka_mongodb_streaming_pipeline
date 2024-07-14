from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
import urllib.parse
from bson.json_util import dumps
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables
load_dotenv()

# MongoDB connection setup
password = os.environ.get('MONGO_DB_PASSWORD')
escaped_password = urllib.parse.quote_plus(password)
conn_str = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn_str)

# Database and collection setup
db = mongo_client['logistic_data']
collection = db['delivery_truck_data']

# Endpoint to render the form for aggregation pipeline input
@app.route('/')
def index():
    return render_template('index.html')

# Endpoint to process aggregation pipeline form submission
@app.route('/aggregate', methods=['POST'])
def aggregate_documents():
    try:
        pipeline = request.form.get('pipeline', '')  # Get pipeline from form data
        pipeline = eval(pipeline)  # Convert string to Python object (dict/list)
        
        # Perform aggregation using the submitted pipeline
        results = list(collection.aggregate(pipeline))
        
        # Return JSON response or render template with results
        return render_template('index.html', results=dumps(results))
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
