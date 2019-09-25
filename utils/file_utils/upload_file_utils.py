from glob import glob
from utils.file_utils.size_comparable_file import SizeComparableFile

import sys
import os
import logging


def get_size_sorted_files(path):

    files_to_upload = []
    for filename in glob(path, recursive=False):
        if not os.path.isfile(filename):
            continue
        try:
            with open(filename, 'rb') as fh:
                filesize = os.fstat(fh.fileno()).st_size
                header = fh.readline()

                if fh.tell() < filesize:
                    file_to_upload = SizeComparableFile(filename=filename, size=filesize, header=header)
                    files_to_upload.append(file_to_upload)
        except:
            logging.error("Something happened while trying to create {}.  {}".format(filename, sys.exc_info()))
    files_to_upload.sort()
    return files_to_upload


def get_files_for_multiprocessing(path):

    files_to_upload = get_size_sorted_files(path)
    files_to_upload.sort(reverse=False)  # asc order

    mid_indx = int(len(files_to_upload)/2)
    big_files = files_to_upload[mid_indx:]
    big_mid_idx = int(len(big_files)/2)
    big_part_one = big_files[0:big_mid_idx]
    big_part_two = big_files[big_mid_idx:]
    big_part_one.sort(reverse=True)

    small_files = files_to_upload[0:mid_indx]
    small_mid_idx = int(len(small_files)/2)
    small_part_one = small_files[0:small_mid_idx]
    small_part_two = small_files[small_mid_idx:]
    small_part_one.sort(reverse=True)

    list_of_lists = [l for l in [big_part_one, big_part_two, small_part_one, small_part_two] if len(l) > 0]

    return list_of_lists

