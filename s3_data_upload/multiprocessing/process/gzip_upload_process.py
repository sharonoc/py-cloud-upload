from utils.s3_utils import s3_utils

import gzip
import os
import psutil
import random
import utils.multiprocessing_utils.mp_constants as const


def upload_part_to_s3(conn, file_part, **kwargs):
    """
    Method to upload data to s3
    :param kwargs: file,
                    bucket,
                    storage (STANDARD, STANDARD_IA, GLACIER),
                    key,
                    headers,
                    compression,
                    conn
    :param conn: closed side of duplex pipe to send results back to parent process
    :param file_part: name of file to be gzipped
    :return: None
    """
    bucket = kwargs['bucket']
    storage = kwargs['storage']
    key = kwargs['key']
    compression = kwargs['compression']
    max_cpu_usage = kwargs['max_cpu_usage']

    gzip_file = file_part + '.gz'

    with open(file=file_part, mode='rb') as f_in:
        conn.send(True)
        size = os.fstat(f_in.fileno()).st_size

        sleep_time = random.randint(0, 5)
        while psutil.cpu_percent(interval=sleep_time) > max_cpu_usage \
                or psutil.virtual_memory().available < 3 * size:
            sleep_time += 1
            if sleep_time > 5:
                sleep_time = random.randint(0, 5)

        with gzip.open(filename=gzip_file, mode='wb', compresslevel=compression) as f_out:
            f_out.write(b''.join(f_in.readlines()))

    if os.path.exists(file_part):
        os.remove(file_part)

    conn.send({'action': const.GZIPPED_PART, 'zip_file': gzip_file, 'file_size': size})

    filename = os.path.basename(gzip_file)
    full_key = "{}/{}".format(key, filename)

    s3_utils.upload_file_to_bucket(file=gzip_file,
                                   bucket=bucket,
                                   storage=storage,
                                   key=full_key)
    os.remove(gzip_file)
    conn.send({'action': const.UPLOADED_PART, 'key': full_key, 'file_size': size})
