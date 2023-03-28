import json
from kafka import KafkaConsumer
import pymongo
from src import configuration as c


"""Initialize Consumer"""
c_dir_d = KafkaConsumer(
    c.topic_dir,
    bootstrap_servers=["cnt7-naya-cdh63:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: x.decode('utf-8')
)


"""Initialize MongoDB"""
# Connect to the MongoDB database
client = pymongo.MongoClient(c.MongoClient)

# Select the database and collection
dir_d = client[c.db_tz][c.col_DIR]

for message in c_dir_d:
    data = message.value
    json_str = data.strip('[').strip(']').strip('"').replace('\\', '')
    doc_dict = json.loads(json_str)
    x = dir_d.insert_one(doc_dict[0])
    print(f'{x.inserted_id} | {dir_d}')


