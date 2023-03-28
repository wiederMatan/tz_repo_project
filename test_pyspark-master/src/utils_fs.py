import re
import json
import pathlib
import os

from kafka import KafkaConsumer
from kafka import KafkaProducer


def send_file_topic(producer, source_file, topic_dest):
    with open(source_file, 'r') as file:
        lines = file.readlines()  # returns list of strings
        producer.send(topic=topic_dest, value=json.dumps(lines).encode('utf-8'))
        producer.flush()

#
# def check_file_exists(directory_path, file_name):
#     file_path = os.path.join(directory_path, file_name)
#     return os.path.isfile(file_path)



# def send_data_mongo(consumer, db_collection):
#     for message in consumer:
#         data = message.value
#         json_str = data.strip('[').strip(']').strip('"').replace('\\', '')
#         doc_dict = json.loads(json_str)
#         x = db_collection.insert_one(doc_dict[0])
#         print(f'{x.inserted_id} | {db_collection}')


