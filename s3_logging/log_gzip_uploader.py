from s3_data_upload import gzip_uploader

import sys
import utils.s3_utils.s3_utils as s3_utils


class LogGZipUploader(gzip_uploader.GZipUploader):

    def __init__(self, args):
        super().__init__(args=args)
        self.data = args.body

    def __new__(cls, args):
        instance = object.__new__(cls)
        instance.__init__(args=args)
        return instance

    def upload_results_to_s3(self):

        try:
            full_key = "{}/{}".format(self.key, self.path)
            s3_utils.upload_part_to_bucket_with_storage_tag(body=self.data,
                                                            bucket=self.bucket,
                                                            storage=self.storage,
                                                            full_key=full_key)
        except:
            raise Exception(sys.exc_info())
