import logging
import os
from pathlib import Path


class my_log:

    def __init__(self, path_log, sw_debug):
        self.path_log = path_log
        self.sw_debug = sw_debug
        if self.sw_debug == 1:
            print(f'Log File:{self.path_log}')

        try:
            if os.path.isfile(self.path_log):
                os.remove(self.path_log)
                if self.sw_debug == 1:
                    print(f'Remove Log File:{path_log}')
        except Exception as error:
            self.log("ERROR", f' ERROR in global_ini :{error}', self.sw_debug)

        ''' Create LOGGER '''
        logging.basicConfig(filename=path_log, format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)
        logging.basicConfig(filename=path_log, format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S', level=logging.ERROR)
        self.log("INFO", '******************* BEGIN my_logger *********************')

    def log( self, s_type, msg):
        if self.sw_debug == 1:
            print(f'Type:{s_type} Msg:{msg}')
        if s_type == "INFO":
            logging.info(msg)
        if s_type == "ERROR":
            logging.error(msg)

