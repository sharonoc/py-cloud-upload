from abc import ABC, abstractmethod
from utils.s3_utils import s3_utils
from multiprocessing import Process, Pipe

import psutil
import logging
import os
import configparser as cp


class GZipUploader(ABC):

    def __init__(self, args):

        dir_config = cp.ConfigParser()
        dir_config.read('./conf/config.ini')
        self.temp_path = dir_config.get('temp', 'DIR')
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)
        self.storage = args.storage
        self.key = args.key
        self.path = (args.file or args.path)
        self.max_uncompressed = args.uncompressed
        self.compression = args.compression
        self.bucket = args.bucket

        self.create_parts_func = None
        self.gzip_upload_func = None
        self.running_processes = {}
        self.gzip_running_size = 0
        self.gzip_file_list = []
        self.file_running_size = 0
        self.max_cpu_usage = args.percent_cpus
        psutil.cpu_percent(interval=None)

        super().__init__()

    @abstractmethod
    def upload_results_to_s3(self):
        pass

    def upload_file(self, file, full_key):
        s3_utils.upload_file_to_bucket(file=file,
                                       bucket=self.bucket,
                                       storage=self.storage,
                                       key=full_key)

    def can_add_file_process(self, f_size):
        """
        Check available memory as to not overwhelm the memory
        :param f_size: size of the file to divide
        :return:
        """
        size = min(self.max_uncompressed, f_size)
        potential_usage = (self.gzip_running_size * 3) + self.file_running_size + size
        # defer to a gzip process if there are any to run
        can_add = potential_usage < psutil.virtual_memory().available \
                  and not (self.gzip_running_size == 0 and len(self.gzip_file_list) > 0)
        if can_add:
            self.file_running_size += size
            logging.debug("Added File process, running size {}".format(self.file_running_size))
        return can_add

    def can_add_gzip_process(self, f_size):
        """
        Average amount of memory needed to gzip a file is 3 times the file size
        We are trying not to overwhelm the host server
        :param f_size: size of file part to gzip
        :return:
        """
        potential_usage = ((self.gzip_running_size + f_size) * 3) + self.file_running_size
        can_add = potential_usage < psutil.virtual_memory().available \
                  and psutil.cpu_percent(interval=0.1) < self.max_cpu_usage * (100 - psutil.cpu_percent())
        return can_add

    def add_gzip_upload_process(self, args, kwargs):
        """
        Create and start a gzip process
        Keep track of the total size of files we are gzipping
        :param args: contains info about file
        :param kwargs:
        :return:
        """
        parent_conn, child_conn = Pipe(duplex=False)

        process = Process(name=os.path.basename(args['file']),
                          target=self.gzip_upload_func,
                          args=[child_conn, args['file_part']],
                          kwargs=kwargs)

        setattr(process, 'process_type', 'GZIP')
        setattr(process, 'header', args['header'])
        setattr(process, 'file', args['file'])
        setattr(process, 'file_size', args['file_size'])

        process.start()
        self.gzip_running_size += args['file_size']
        logging.debug("Added Gzip process, running size {}".format(self.gzip_running_size))
        child_conn.close()
        try:
            parent_conn.poll()
            started = parent_conn.recv()
            if started:
                logging.debug("GZIP and upload process started on {}".format(args['file_part']))
        except (EOFError, BrokenPipeError):
            pass
        return process, parent_conn

