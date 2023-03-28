from src import configuration as co
from src import utils_fs as ut
from src import my_logger as lg
from os import listdir
from os.path import isfile, join
import shutil
import time
import os



sw_debug = 1
PROJECT_PATH = lg.Path(__file__).absolute().parent.parent
path_log = f'{PROJECT_PATH}/logs/log_producer.log'
logger = lg.my_log(path_log, sw_debug)

"""Initialize Producer"""
producer = ut.KafkaProducer(
    bootstrap_servers=co.bootstrap_servers,
    client_id='producer',
    acks=1,
    compression_type=None,
    retries=3)

while True:
    # Read files from source directory
    json_files = [os.path.join(co.source_file, f) for f in os.listdir(co.source_file) if os.path.isfile(os.path.join(co.source_file, f))]
    logger.info(f"Found {len(json_files)} files in source directory")

    if json_files:
        # Process each file
        for file_name in json_files:
            logger.info(f"Processing file: {file_name}")

            if co.filter_info in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info)
                source_path = file_name
                dest_path = os.path.join(co.archive_files, os.path.basename(file_name))
                if os.path.isfile(source_path):
                    shutil.move(source_path, dest_path)

            elif co.filter_info_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info_dir)
                source_path = file_name
                dest_path = os.path.join(co.archive_files, os.path.basename(file_name))
                if os.path.isfile(source_path):
                    shutil.move(source_path, dest_path)

            elif co.filter_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_dir)
                source_path = file_name
                dest_path = os.path.join(co.archive_files, os.path.basename(file_name))
                if os.path.isfile(source_path):
                    shutil.move(source_path, dest_path)

        # Remove processed files from the list
        json_files = [f for f in json_files if f not in os.listdir(co.archive_files)]
    else:
        logger.info("No files found in source directory")
        time.sleep(100)
