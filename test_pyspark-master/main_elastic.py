import src.global_ini as gf_i
#import src.global_fc as gf
import src.elastic_functions as ef
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.helpers import BulkIndexError
from pprint import pprint

import sys

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client[gf_i.DATABASE]

# Connect to Elasticsearch
es_client = Elasticsearch("http://localhost:9200/")

#------------------------------------ MONGO_COL_INFO ----------------------------------
# Connect to MongoDB  - collection
mongo_collection = mongo_db[gf_i.MONGO_COL_INFO]

# Assume we have a collection named 'my_collection'
# and we want to get all the distinct values of a field named 'my_field'
column_name = "computer_name"
distinct_values = mongo_collection.distinct(column_name)
for value in distinct_values:
    column = "computer_name"
    i_es_index = f"myindex_info_{value}".lower()

    ef.chk_and_create_es_index(es_client,i_es_index, ef.mapping_info)
    last_now = ef.get_latest_value_for_grp("now", column_name, value, es_client, i_es_index)

    # Query MongoDB for new documents
    if last_now is None:
        mongo_query = {column_name: value, "disc_free_precent": {"$exists": True}}
    else:
        mongo_query = {column_name: value, "now": {"$gt": last_now}, "disc_free_precent": {"$exists": True}}

    # Get data from MongoDb and create pymongo.cursor
    mongo_docs = mongo_collection.find(mongo_query).sort("now", 1)#.limit(1)#.limit(3) 1-asc / -1 - desc
    i_actions = [ef.doc_to_action_info(doc, i_es_index) for doc in mongo_docs]
    ef.insert_data_to_es(es_client,i_actions)
    print(f" value: {value} last_now: {last_now} action:{len(i_actions)}")

#------------------------------------ MONGO_COL_F_INFO ----------------------------------
# Connect to MongoDB  - collection
mongo_collection = mongo_db[gf_i.MONGO_COL_F_INFO]

# Assume we have a collection named 'my_collection'
# and we want to get all the distinct values of a field named 'my_field'
column_name = "computer_name"
distinct_values = mongo_collection.distinct(column_name)
for value in distinct_values:
    column = "computer_name"
    i_es_index = f"myindex_finfo_{value}".lower()

    ef.chk_and_create_es_index(es_client, i_es_index, ef.mapping_f_info)
    last_now = ef.get_latest_value_for_grp("now_run", column_name, value, es_client, i_es_index)

    # Query MongoDB for new documents
    if last_now is None:
        mongo_query = {column_name: value, "computer_name": {"$exists": True}}
    else:
        mongo_query = {column_name: value, "now_run": {"$gt": last_now}, "computer_name": {"$exists": True}}

    # Get data from MongoDb and create pymongo.cursor
    mongo_docs = mongo_collection.find(mongo_query).sort("now_run",1).limit(1)#.limit(3) 1-asc / -1 - desc
    i_actions = [ef.doc_to_action_f_info(doc, i_es_index) for doc in mongo_docs]

    print(f" value: {value} last_now: {last_now} action:{len(i_actions)}")

    ef.insert_data_to_es(es_client,i_actions)

sys.exit()

# print("___OK END---")