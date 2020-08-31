import os 
from bson import json_util
import json
from flask import Flask , render_template, jsonify , make_response,request
from flask_restful import Resource, Api
from flask_cors import CORS, cross_origin

import pymongo
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["hackenews"]
metrics_collection = db["metrics"]

def create_app():
	app = Flask(__name__)
	app.config.from_object(os.environ["APP_SETTINGS"])
	cors = CORS(app)
	app.config['CORS_HEADERS'] = 'Content-Type'
	return app


app = create_app()
api = Api(app)


@app.route("/",methods=["GET","POST"])
def index():
    return render_template("index.html")

@app.route("/getmetric",methods=["GET"])
@cross_origin()
def getmetric():
	metric = request.args.get('metric')
	results = metrics_collection.find_one({"metric":metric})
	del results["_id"]
	return {'results':json.loads(json.dumps(results))}


