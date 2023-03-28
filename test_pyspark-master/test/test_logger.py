from src import configuration as co
from src import utils_fs as ut
from datetime import datetime
from src import my_logger as lg
import time

sw_debug = 1
PROJECT_PATH = lg.Path(__file__).absolute().parent.parent

path_log = f'{PROJECT_PATH}/logs/log_producer.log'
l = lg.my_log(path_log, sw_debug)