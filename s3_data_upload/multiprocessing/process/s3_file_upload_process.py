from time import sleep

import os
import pickle
import psutil
import random
import shutil
import utils.multiprocessing_utils.mp_constants as const


def create_file_parts(file_pickle, conn, **kwargs):
    """
    Divides the file into parts based on the uncompressed arg and places the parts in the temp dir
    :param file_pickle: pickled file object for dividing
    :param conn: multiprocessing.Pipe used to send info back to the parent
    :param kwargs: args map for additional info not in the file pickle
    :return: none
    """
    unpickled_file = pickle.loads(file_pickle)
    bytes_per_file = kwargs['max_uncompressed']
    temp_dir = kwargs['temp_dir']
    p = 0

    if os.path.exists(unpickled_file.filename):
        conn.send(True)
        name, orig_ext = os.path.basename(unpickled_file.filename).split('.')
        new_file = os.path.join(temp_dir, "{}_part{}.{}".format(name, p, orig_ext))

        with open(unpickled_file.filename, 'rb') as f_in:
            size = os.fstat(f_in.fileno()).st_size

            # if the file is less than max just copy it
            if size <= bytes_per_file:
                header = f_in.readline()
                shutil.copyfile(unpickled_file.filename, new_file)
                conn.send({'action': const.CREATED_PART,
                           'file': unpickled_file.filename,
                           'file_part': new_file,
                           'file_size': size,
                           'header': header})
            else:
                header = f_in.readline()

                while f_in.tell() < size:

                    sleep_time = random.randint(0, 5)
                    sleep(sleep_time)
                    while psutil.virtual_memory().available < bytes_per_file:
                        if sleep_time > 5:
                            sleep_time = random.randint(0, 5)
                        sleep(sleep_time)
                        sleep_time += 1

                    with open(file=new_file, mode='wb') as f_out:
                        f_out.write(header)
                        f_out.writelines(f_in.readlines(bytes_per_file))
                        new_size = os.fstat(f_out.fileno()).st_size

                    conn.send({'action': const.CREATED_PART,
                               'file': unpickled_file.filename,
                               'file_part': new_file,
                               'file_size': new_size,
                               'header': header})
                    p += 1
                    new_file = os.path.join(temp_dir, "{}_part{}.{}".format(name, p, orig_ext))

        conn.send({'action': const.FINISHED_CREATING_PARTS, 'file': unpickled_file.filename, 'file_size': size})
