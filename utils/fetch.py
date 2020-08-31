from google.cloud import bigquery 
from bigqueries import queries
import os

import pymongo
client = pymongo.MongoClient(os.environ["DATABASE_URL"])
db = client["hackenews"]
metrics_collection = db["metrics"]


def fetch_metric(metric):
	client = bigquery.Client()
	query_job = client.query(queries[metric],
	                        job_config=bigquery.QueryJobConfig(labels={"metric": metric.strip("_").lower()}),
	                        job_id_prefix="hn_metrics_"
	                        )

	results = query_job.result()
	field_names = [f.name for f in results.schema if f.name != "month"]
	dict_result = {"metric":metric}
	for row in results:
		for field in field_names:
			dict_result[str(row.month)] = row[field]
	
	remove_log = metrics_collection.delete_one({"metric":metric})
	insert_log = metrics_collection.insert_one(dict_result)
	


if __name__ == "__main__":
	for metric in queries.keys():
		fetch_metric(metric)






