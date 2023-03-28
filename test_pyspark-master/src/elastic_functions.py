import logging
from datetime import datetime
from elasticsearch.helpers import BulkIndexError
from elasticsearch.helpers import bulk

# Define the Elasticsearch mapping
mapping_info = {
    "mappings": {
        "properties": {
            "computer_name": {"type": "text"},
            "disc_total" : {"type": "long"},
            "disc_used":{"type": "long"},
            "disc_free":{"type": "long"},
            "disc_free_precent":{"type": "float"},
            "now": {"type": "date"}
        }
    }
}

mapping_f_info = {
    "mappings": {
        "properties": {
            "computer_name": {"type": "text"},
            "folder_name": {"type": "text"},
            "folder_size": {"type": "long"},
            "fnm_with_last_change": {"type": "text"},
            "chk_from_date": {"type": "date"},
            "count_all_files": {"type": "long"},
            "count_last_files": {"type": "long"},
            "count_err_files": {"type": "long"},
            "now_run": {"type": "date"}
        }
    }
}


# Get the latest "now" value from Elasticsearch
def get_latest_value(column, ec, ies_index): # for "now"
    query = {"query": {"match_all": {}}, "sort": {column: {"order": "desc"}}, "size": 1}
    res = ec.search(index=ies_index, body=query)
    if res["hits"]["total"]["value"] == 0:
        latest_now = None
    else:
        latest_now = res["hits"]["hits"][0]["_source"]["now"]
    logging.info(f"f:get_latest_value latest_now:{latest_now}")
    return latest_now


# Define the Elasticsearch query to find the latest document that matches the given column and value
def get_latest_value_for_grp(column, grp_nm, grp_value, ec, ies_index):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {grp_nm: grp_value}}
                ]
            }
        },
        "size": 1,
        "sort": {column: {"order": "desc"}}
    }
    res = ec.search(index=ies_index, body=query)
    if res["hits"]["total"]["value"] == 0:
        latest_now = None
    else:
        latest_now = res["hits"]["hits"][0]["_source"][column]
    logging.info(f"f:get_latest_value_for_grp(..{grp_nm},{grp_value}..) latest_now:{latest_now}")
    return latest_now

# Convert MongoDB documents to Elasticsearch actions
def doc_to_action_info(doc, es_index):
    action = {
        "_index": es_index,
        "_source": {
            "computer_name": doc["computer_name"],
            "disc_total": doc["disc_total"],
            "disc_used": doc["disc_used"],
            "disc_free": doc["disc_free"],
            "disc_free_precent": doc["disc_free_precent"],
            "now": datetime.strptime(doc["now"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
    }
    return action

def doc_to_action_f_info(doc, es_index):
    action = {
        "_index": es_index,
        "_source": {
            "computer_name": doc["computer_name"],
            "folder_name": doc["folder_name"],
            "folder_size": doc["folder_size"],
            "count_all_files": doc["count_all_files"],
            "count_last_files": doc["count_last_files"],
            "now_run": datetime.strptime(doc["now_run"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
    }
    return action


# Create the Elasticsearch index if it doesn't exist
def chk_and_create_es_index(es_client, i_es_index, mapping):
    if not es_client.indices.exists(index=i_es_index):
        logging.info(f"index not exist =>{i_es_index}")
        es_client.indices.create(index=i_es_index, body=mapping)
    else:
        logging.info(f"index exist =>{i_es_index}")


# Insert the new documents into Elasticsearch
def insert_data_to_es(es_client,i_actions):
    try:
        bulk(es_client, i_actions)
        logging.info("BULK OK")
        return 1
    except BulkIndexError as e:
        for error in e.errors:
            logging.error(f"BULK ERROR:{error}")
        return 0

