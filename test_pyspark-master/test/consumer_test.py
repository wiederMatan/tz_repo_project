import json
from kafka import KafkaConsumer
import pymongo
from src import configuration as c
from src import utils_fs as ut
from time import sleep



"""Initialize Consummer"""
consumer = KafkaConsumer(
    c.topic_info,
    bootstrap_servers=["cnt7-naya-cdh63:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: x.decode('utf-8')
)


for message in consumer:
    data = message.value
    json_str = data.strip('[').strip(']').strip('"').replace('\\', '')
    doc_dict = json.loads(json_str)
    x = db_collection.insert_one(doc_dict[0])
    print(f'{x.inserted_id} | {db_collection}')



"""Initialize MongoDB"""
# Connect to the MongoDB database
client = pymongo.MongoClient(c.MongoClient)

# Select the database and collection
main_info = client[c.db_tz][c.col_INFO]
info_dir = client[c.db_tz][c.col_F_INFO]
dir_d = client[c.db_tz][c.col_DIR]


ut.send_data_mongo(c_main_info, main_info)
ut.send_data_mongo(c_info_dir, info_dir)
ut.send_data_mongo(c_dir_d, dir_d)



