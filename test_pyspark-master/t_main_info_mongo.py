import json
from kafka import KafkaConsumer
import pymongo
from src import configuration as c


"""Initialize Consumer"""
c_main_info = KafkaConsumer(
    c.topic_info,
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
main_info = client[c.db_tz][c.col_INFO]

for message in c_main_info:
    data = message.value
    json_str = data.strip('[').strip(']').strip('"').replace('\\', '')
    doc_dict = json.loads(json_str)
    x = main_info.insert_one(doc_dict[0])
    print(f'{x.inserted_id} | {main_info}')



