from utils.file_utils import upload_file_utils
from s3_data_upload import gzip_uploader
from multiprocessing.connection import wait
from s3_data_upload.multiprocessing.process.gzip_upload_process import upload_part_to_s3
from s3_data_upload.multiprocessing.process.s3_file_upload_process import create_file_parts

import pickle
import multiprocessing
import os
import logging
import sys
import utils.multiprocessing_utils.mp_constants as const


class S3FileUploadProcessRunner(gzip_uploader.GZipUploader):

    def __new__(cls, args):

        instance = object.__new__(cls)
        instance.__init__(args=args)
        return instance

    def __init__(self, args):

        super().__init__(args=args)
        self.gzip_upload_func = upload_part_to_s3
        self.create_parts_func = create_file_parts

    def upload_results_to_s3(self):
        """
        Run a separate process for each file that is to be uploaded to s3
        :return:
        """
        try:
            file_lists = upload_file_utils.get_files_for_multiprocessing(path=self.path)
            num_files = sum(len(s) for s in file_lists)
            logging.debug("Num files found {}".format(num_files))
            kwargs = {'bucket': self.bucket,
                      'key': self.key,
                      'compression': self.compression,
                      'max_uncompressed': self.max_uncompressed,
                      'max_cpu_usage': self.max_cpu_usage,
                      'storage': self.storage,
                      'temp_dir': self.temp_path}

            i = 0

            # While there is anything to process keep going
            while any(file_lists) or any(self.gzip_file_list) or any(self.running_processes):
                """
                Check the gzip list first because gzipping is the longest running process so we want 
                to run as many gzip processes as we can at a time
                Otherwise we are keeping track of the order of processes
                
                """
                for gzip_file in self.gzip_file_list:
                    f_size = gzip_file['file_size']
                    if self.can_add_gzip_process(f_size=f_size):
                        self.gzip_file_list.remove(gzip_file)
                        process, parent_conn = self.add_gzip_upload_process(args=gzip_file,
                                                                            kwargs=kwargs)
                        self.running_processes[parent_conn] = {'process': process,
                                                               'size': f_size}
                """
                Check all the processes to see if any have reported back that they are finished
                If gzip is finished we want to release the memory we are tracking in order to allow
                more gzip processes to run
                """
                if any(self.running_processes):
                    keys = list(self.running_processes.keys())
                    while len(keys) > 0:
                        # there is a maximum of 63 elements in a list for the wait call
                        wait_keys = keys[0:min(len(keys), 62)]
                        keys = keys[min(len(keys), 62):]
                        for listener in wait(wait_keys, timeout=0.01):
                            try:
                                while listener.poll(0.01):
                                    response = dict(listener.recv())
                                    logging.info(response)

                                    if response['action'] == const.CREATED_PART:
                                        self.gzip_file_list.append(response)
                                    elif response['action'] == const.GZIPPED_PART:
                                        self.gzip_running_size -= response['file_size']
                                    elif response['action'] == const.FINISHED_CREATING_PARTS:
                                        self.file_running_size -= min(response['file_size'], self.max_uncompressed)
                                    elif response['action'] == const.UPLOADED_PART:
                                        pass
                                    else:
                                        logging.error("UNKNOWN RESPONSE {}".format(response))
                            except (EOFError, BrokenPipeError):
                                pass

                            proc_dict = self.running_processes.get(listener)
                            """
                            Kill dead processes
                            """
                            if not proc_dict['process'].is_alive():
                                current_proc = self.running_processes.pop(listener)['process']
                                current_proc.join()
                                current_proc.terminate()

                if any(file_lists):
                    f_size = file_lists[i][-1].size
                    if self.can_add_file_process(f_size=f_size):
                        file_to_upload = file_lists[i].pop()
                        process, parent_conn = self.add_divide_file_process(file_to_upload=file_to_upload,
                                                                            kwargs=kwargs)
                        self.running_processes[parent_conn] = {'process': process, 'size': f_size}
                        if not any(file_lists[i]):
                            file_lists.remove(file_lists[i])
                        i = i + 1 if i + 1 < len(file_lists) else 0

            if not any(self.running_processes) and not any(file_lists) and not any(self.gzip_file_list):
                logging.debug("All processes finished")

        except (FileExistsError, FloatingPointError) as e:
            raise e
        except:
            raise Exception(sys.exc_info())

    def add_divide_file_process(self, file_to_upload, kwargs):
        """
        Create and start a process for dividing the file into n size parts
        File must be pickled in order to initiate a process
        :param file_to_upload: full path of file to be divided
        :param kwargs:
        :return:
        """
        parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
        process = multiprocessing.Process(name=os.path.basename(file_to_upload.filename),
                                          target=self.create_parts_func,
                                          args=[pickle.dumps(file_to_upload), child_conn],
                                          kwargs=kwargs)

        setattr(process, 'process_type', 'DIVIDE_FILE')
        process.start()
        child_conn.close()
        try:
            parent_conn.poll()
            started = parent_conn.recv()
            if started:
                logging.debug("FILE Process started {}".format(process.name))
        except (EOFError, BrokenPipeError):
            pass
        return process, parent_conn
