import boto3


def get_s3_client():
    try:
        s3client = boto3.client('s3')
        return s3client
    except:
        raise


def get_storage_tag(storage, add_tag_after_upload):
    """
    Create glacier storage tag for lifecycle rule
    :param storage:
    :param add_tag_after_upload: Format of tagging is different based on how object is uploaded to s3
    :return: storage = STANDARD_IA if it was GLACIER else storage, correct tag format for glacier tag
    """
    tag = ''

    if storage == 'GLACIER':
        storage = 'STANDARD_IA'
        if not add_tag_after_upload:
            tag = "storage=glacier"
        else:
            tag = {'Key': 'storage', 'Value': 'glacier'}
    elif storage == 'LOG':
        storage = 'STANDARD_IA'
        if not add_tag_after_upload:
            tag = "storage=log"
        else:
            tag = {'Key': 'storage', 'Value': 'log'}

    return storage, tag


def upload_file_to_bucket(file, bucket, storage, key):
    """
    Non-multiprocessing method to upload files to s3
    :param file: Full pathname of the file
    :param bucket: bucket to load to
    :param storage: s3 storage type
    :param key: path in bucket for file
    :return: none
    """
    try:
        s3 = get_s3_client()

        storage, tag = get_storage_tag(storage=storage, add_tag_after_upload=True)

        s3.upload_file(Filename=file,
                       Bucket=bucket,
                       Key=key,
                       ExtraArgs={
                           "StorageClass": storage,
                           "ContentEncoding": 'gzip'
                       }
                       )

        if any(tag):
            s3.put_object_tagging(Bucket=bucket,
                                  Key=key,
                                  Tagging={'TagSet': [tag]}
                                  )
    except:
        raise


def upload_part_to_bucket_with_storage_tag(body, bucket, storage, full_key):
    """
    Method to upload data to s3
    :param body: binary data
    :param bucket: s3 bucket
    :param storage: STANDARD, STANDARD_IA, GLACIER
    :param full_key: full name of file
    :return: response from s3 put operation
    """
    try:

        s3 = get_s3_client()

        storage, tag = get_storage_tag(storage=storage, add_tag_after_upload=False)

        s3.put_object(Body=body,
                      Bucket=bucket,
                      StorageClass=storage,
                      Tagging=tag,
                      Key=full_key)
    except:
        raise
