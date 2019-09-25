from s3_logging import logging_setup
from s3_data_upload.multiprocessing.process_runner.s3_file_upload_process_runner import S3FileUploadProcessRunner

import argparse
import sys
import logging
import os


def get_command_line_args():
    """
    This method uses python argparse.Argument parser to extract the command line args
    :return: args dict
    """
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--bucket", "-b",
                            help="S3 bucket for transfer",
                            required=True
                            )

    arg_parser.add_argument("--compression", "-c",
                            help="Compression level for gzip, higher number equals slower but more compact compression",
                            required=False,
                            choices=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                            default=6,
                            type=int
                            )

    arg_parser.add_argument("--file", "-f",
                            help="""Full path to a file or directory of files to be uploaded""",
                            required=True
                            )

    arg_parser.add_argument("--key", "-k",
                            help="S3 path to load the files to in the bucket",
                            required=False
                            )

    arg_parser.add_argument("--percent_cpus", "-p",
                            help="""Percentage of cpu usage to use during multiprocessing phase 
                            as a whole number.  If the value is 100 then multiprocessing_utils will use all cpu power
                            available. The amount of available virtual memory will also be taken into consideration due 
                            to the high memory demands of compressing the data before uploading to s3""",
                            required=False,
                            default=80,
                            type=int)

    arg_parser.add_argument("--storage", "-s",
                            help="""Either GLACIER, STANDARD or STANDARD_IA,
                                 if GLACIER then file(s) will be tagged as 'storage=glacier',
                                 you need to ensure that the bucket has a lifecycle rule
                                 to change the storage class based on this tag.""",
                            choices=['GLACIER', 'STANDARD', 'STANDARD_IA'],
                            default='STANDARD_IA',
                            required=False
                            )

    arg_parser.add_argument("--uncompressed", "-u",
                            help="Max size of uncompressed data in bytes.  Default is 1G",
                            required=False,
                            default=1073742000,
                            type=int
                            )

    args = arg_parser.parse_args()
    return args


def main():
    args = get_command_line_args()
    filename = os.path.basename(args.file)
    logging_setup.set_up_logging(app="delimited_file_transfer", file=filename)
    try:
        # Process all files in a directory spawning a new process per file
        uploader = S3FileUploadProcessRunner(args=args)
        uploader.upload_results_to_s3()
    except:
        logging.error(sys.exc_info())
        exit(1)
    finally:
        logging_setup.finish_logging()


if __name__ == '__main__':
    main()
    exit(0)
