import os
import shutil
import time
from pathlib import Path
from kafka import KafkaProducer

from src import configuration as co
from src import utils_fs as ut
from src import my_logger as lg

PROJECT_PATH = Path(__file__).resolve().parents[1]
path_log = PROJECT_PATH / 'logs' / 'log_producer.log'

l = lg.my_log(path_log, sw_debug=1)

producer = ut.KafkaProducer(
    bootstrap_servers=co.bootstrap_servers,
    client_id='producer',
    acks=1,
    compression_type=None,
    retries=3
)

while True:
    source_dir = Path(co.source_file)

    if not source_dir.exists():
        l.log("ERROR", f"PRODUCER: Source directory {source_dir} does not exist")
        continue

    json_files = [file.path for file in os.scandir(source_dir) if file.is_file() and file.name.endswith('.txt')]

    l.log("INFO", f"PRODUCER: count files =>{len(json_files)}")

    if json_files:
        for file_name in json_files:
            l.log("INFO", f"PRODUCER(1): {file_name}")

            dest_file = Path(co.archive_files) / Path(file_name).name
            if dest_file.exists():
                l.log("WARNING", f"PRODUCER: {dest_file} already exists")
                os.remove(file_name)
                continue

            if co.filter_info in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info)
                shutil.move(file_name, dest_file)

            elif co.filter_info_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info_dir)
                shutil.move(file_name, dest_file)

            elif co.filter_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_dir)
                shutil.move(file_name, dest_file)

    else:
        time.sleep(60)
