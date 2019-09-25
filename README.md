AWS S3 BUCKET SETUP:  (not necessary for testing)
create bucket
create lifecycle rules on bucket:
Move_to_Glacier:
    Scope: Whole bucket , Tags : [ Key storage , Value : glacier ]
    Transition: current version: transition to amazon glacier after 0 days
    Expiration: None

Create and download the rootkey.csv file from your AWS account
Put this file in a safe place on your computer
Create the AWS config for boto3:
1. Create ".aws" directory in a safe location on your computer
2. Create an empty "config" file (no extension) and put it in the ".aws" dir
3. Create an empty (default) config structure in the config file
[default]
aws_access_key_id=
aws_secret_access_key=
region=
4. Fill in the sections with the values from your rootkey.csv that you downloaded earlier, region is your default region.
5. Create an ENVIRONMENT variable ON YOU MACHINE TO POINT boto3 to this config file
AWS_CONFIG_FILE=<your path to>/.aws/config


Required packages:
1. Python 3.7
1. psutil
1. boto3

Set up logging:
1. Configure 'LOG_BUCKET' in config.ini to where you want the debug and error logs to be posted.
They will be posted to <bucket>/LOGS/

Finish configuration:
1. Configure the location for the temp files that are generated during the process.
Default:
[temp]
DIR: ./upload/temp


 ******************************************************************************************************************
The performance of this application is memory and network bound.
Increasing the compression level will make things take longer.
Increasing the amount of uncompressed bytes will cause compression to take longer but will reduce I/O as fewer files
will be uploaded to S3.
Larger uncompressed bytes will also have increased memory demands for the compression phase.


*************************************************************************************************
FILE Uploader:
Files are uploaded in gzip compression format.
The amount of available virtual memory is tracked during the compression phase as to not exceed the amout reported by 
psutil.virtual_memory().available

******************************************************************************************************************************

UPLOADING FILES

delimited_file_upload.py

Command line args:
------------------
                        
1. --bucket, -b S3 bucket for transfer
                        
1. --compression, -c {1,2,3,4,5,6,7,8,9}
                        Compression level for gzip, higher number equals
                        slower but more compact compression
                        
1. --file, -f FILE  FILE uploader: Full path to a file or directory of
                        files to be uploaded
                        
1. --key, -k KEY     S3 path within the stage where the files to load exist
                    For instance you may want your files in 'FILES/20019_12_01'
  
1. --percent_cpus, -p PERCENT_CPUS
                        Percentage of cpu usage to use during multiprocessing
                        phase as a whole number. If the value is 100 then
                        multiprocessing_utils will use all cpu power
                        available. 
                        
1. --move_orig, -o MOVE_ORIG
                        Path of directory on source server to move processed
                        file to.
                        
1. --processed_key PROCESSED_KEY, -p PROCESSED_KEY
                        Folder on S3 to move files to once they are loaded
                        into Snowflake. If blank files will not be moved
                        
1. --storage {GLACIER,STANDARD,STANDARD_IA}, -s {GLACIER,STANDARD,STANDARD_IA}
                        Either GLACIER, STANDARD or STANDARD_IA, if GLACIER
                        then file(s) will be tagged as 'storage=glacier', you
                        need to ensure that the bucket has a lifecycle rule to
                        change the storage class based on this tag.
                        
1. --uncompressed, -u UNCOMPRESSED
                        Max size of uncompressed file parts in bytes. Default is 1G

Example execution:
-b py-data-upload -f ~/dev/Test.csv -k FILES -u 104857600 -p 50

If you need to generate a large test file I suggest, I found it very useful.
If I needed a file larger than the max it created I just tweaked it to open the file in subsequent run with 'a' flag
and comment out the header write.  This way I easily created a multi gig file with useful example data
* https://towardsdatascience.com/generation-of-large-csv-data-using-python-faker-8cfcbedca7a7
