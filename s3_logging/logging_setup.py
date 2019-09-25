from datetime import datetime
from argparse import Namespace

import logging.handlers
import logging
import json
import sys
import logging.config as log_config
import configparser as cp
import s3_logging.log_gzip_uploader as log_gzip_uploader

ONE_GIG = 1024 * 1024 * 1024


class S3LoggingHandler(logging.Handler):

    def __init__(self, level, app, bucket, file):
        timestamp_parts = str(datetime.now()).split( )
        today = timestamp_parts[0].replace('-', '_')
        time = timestamp_parts[1].split('.')[0].replace(':', '_')
        self.buffer = []
        self.key = 'LOGS/{}/{}/{}'.format(app, logging.getLevelName(level=level), today)
        self.bucket = bucket
        super().__init__(level=level)
        self.name = '{}_{}.log'.format(file, time)

    def emit(self, record):
        super().emit(record=record)

    def handle(self, record):
        self.buffer.append(record)

    def flush(self):
        if len(self.buffer) > 0:
            body = b'\n'.join(self.format(line).encode('utf-8') for line in self.buffer)
            args = Namespace(body=body,
                             storage='LOG',
                             bucket=self.bucket,
                             key=self.key,
                             file=self.name,
                             uncompressed=ONE_GIG,
                             compression=6,
                             percent_cpus=1)
            uploader = log_gzip_uploader.LogGZipUploader(args=args)
            uploader.upload_results_to_s3()
        self.buffer = []
        super().flush()

    def close(self):
        self.flush()


def set_up_logging(app, file):
    try:
        #  Set third party packages to ERROR so debugging is not so noisy
        logging.getLogger('s3transfer').setLevel('ERROR')
        for logger in logging.root.manager.loggerDict.keys():
            if logger not in ['stream']:
                logging.getLogger(logger).setLevel('ERROR')

        logging_config = cp.ConfigParser()
        logging_config.read('./conf/config.ini')

        log_config_file = './conf/log_config.json'

        with open(log_config_file) as f:
            config_dict = json.load(f)
            log_config.dictConfig(config_dict)

        log_formatter = logging.getLogger().handlers[0].formatter
        log_bucket = logging_config.get('logging_config', 'LOG_BUCKET')

        debug_handler = S3LoggingHandler(level=logging.DEBUG,
                                         app=app,
                                         bucket=log_bucket,
                                         file=file)
        debug_handler.setFormatter(log_formatter)
        debug_mem_handler = logging.handlers.MemoryHandler(capacity=ONE_GIG,
                                                           flushLevel=logging.DEBUG,
                                                           target=debug_handler)
        debug_mem_handler.setLevel(logging.DEBUG)
        debug_mem_handler.name = "DEBUG_MEM_HANDLER"
        logging.getLogger().addHandler(debug_mem_handler)

        error_handler = S3LoggingHandler(level=logging.ERROR,
                                         app=app,
                                         bucket=log_bucket,
                                         file=file)
        error_handler.setFormatter(log_formatter)
        error_mem_handler = logging.handlers.MemoryHandler(capacity=ONE_GIG,
                                                           flushLevel=logging.ERROR,
                                                           target=error_handler)
        error_mem_handler.setLevel(logging.ERROR)
        error_mem_handler.name = "ERROR_MEM_HANDLER"
        logging.getLogger().addHandler(error_mem_handler)

        log_level = logging_config.get('logging_config', 'LOG_LEVEL')
        logging.getLogger('stream').setLevel(level=log_level)

    except:
        raise Exception(sys.exc_info())


def finish_logging():

    for handler in logging.getLogger().handlers:
        if handler.name in ['ERROR_MEM_HANDLER', 'DEBUG_MEM_HANDLER']:
            target_handler = handler.target
            target_handler.flush()
