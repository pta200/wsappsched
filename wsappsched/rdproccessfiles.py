import logging
import os
import stat
import glob
import threading
import time
import boto3
from pathlib import Path

from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED

logger = logging.getLogger(__name__)

# Set the desired multipart threshold value (5GB)
GB = 1024 ** 3
max_workers=8
config = TransferConfig(multipart_threshold=5*GB, max_concurrency=max_workers)
storage_class="STANDARD"
class Boto3_Worker():

    def __init__(self, file, bucket, root_prefix):
        """initialize boto3 worker

        Args:
            file (str): file path
            job_id (str): flux job id
            bucket (str): s3 bucket name
        """
        self._root_prefix = root_prefix
        self._bucket = bucket
        self._file = file
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._last_progress_update = 0

    def __call__(self, bytes_amount):
        """callback function invoked by boto3 client during upload_file processing
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-callback-parameter

        Args:
            bytes_amount (float): bytes transferred up to the called point
        """
        with self._lock:
            self._seen_so_far += bytes_amount          
            # report bytes/second every 10 function calls per file
            if self._last_progress_update % 50 == 0:
                current = time.time()
                delta = current - self._start_time
                logger.info("speed is %sB/s while uploading %s", int(self._seen_so_far / delta), self._file)
            self._last_progress_update += 1


    def __gen_s3_object_key(self):
        """generate s3 object key prefix

        Returns:
            str: s3 key prefix
        """
        return f"{self._root_prefix}/{self._file.removeprefix('/')}"


    def __gen_file_metadata(self):
        """generate metadata directory from file stat see AWS docs for details
        https://docs.aws.amazon.com/fsx/latest/LustreGuide/posix-metadata-support.html

        Args:
            file (str): file path

        Returns:
            dict: posix metadata
        """
        file_stat = os.stat(self._file)

        file_type = stat.S_IFMT(file_stat.st_mode)
        permissions = stat.S_IMODE(file_stat.st_mode)
        type_octal = oct(file_type)[2:].zfill(1)
        perm_octal = oct(permissions)[2:].zfill(3)

        return {
            "user-agent": "aws-fsx-lustre",
            "file-atime": f"{file_stat.st_atime_ns}ns",
            "file-mtime": f"{file_stat.st_mtime_ns}ns",
            "file-permissions": f"{type_octal}{perm_octal}",
            "file-owner": str(file_stat.st_uid),
            "file-group": str(file_stat.st_gid),
        }



    def upload_s3(self, s3_client):
        """upload single file to s3 include posix metadata and tags

        Args:
            s3_client (Boto3): Boto3 S3 client instance
            file_name (str): file to be uploaded includes full path
            bucket (str): bucket name
            key (str): S3 object key

        Returns:
            tuple: returns transfer success/failure and file
        """
        key = self.__gen_s3_object_key()
        meta = self.__gen_file_metadata()
        try:
            logger.info("upload %s", self._file)
            s3_client.upload_file(
                Filename=self._file,
                Bucket=self._bucket,
                Key=key,
                ExtraArgs={
                    "StorageClass": storage_class,
                    "Metadata": meta,
                    },
                Callback=self,
                Config=config
            )

        except Exception as e:
            # file upload failed)
            logger.error(e)
            return False, self._file
        
        # return True, self._file
        return True, key, meta

def check_and_update(key, bucket, s3_client):
    posix_metadata = {
        "user-agent": "aws-fsx-lustre",
        'file-owner': '1820364143',
        'file-group': '1820364315',
        'file-permissions': '042770' # Example: rwxr-xr-x for directory
    }
    logger.info(f"Fix this key path {key}")
    # The metadata must be included when the object is *created* or *copied*
    # You can update metadata on an existing object by copying it to itself with new metadata

    try:
        # check if folder exists and has permissions
        data = s3_client.head_object(
                Bucket=bucket,
                Key=key,
            )
        # logger.debug(data)

        if data.get("Metadata") and data.get("Metadata") == posix_metadata:
            logger.info(f"key path {key} already exists with same posix permissions")
            return False

    except Exception as e:
        # An error occurred (404) when calling the HeadObject operation: Not Found
        # if the folder does not exists the head_object will throw an exception
        logger.error(e)

    try:
        # create the folder object with posix metadata
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Metadata=posix_metadata,
            Body=b'' # Empty body to simulate an empty folder object
        )
        logger.info(f"Folder '{key}' created in bucket '{bucket}'")
        return True

    except Exception as e:
        print(f"Error updating metadata: {e}")

def process_files(bucket: str, filepath: str, root_prefix: str):
    """upload files to S3 bucket

    Args:
        bucket (str): s3 bucket
        filepath (str): filepath to glob
        root_prefix (str):  object key root prefix
    """

    s3_client = boto3.client('s3')
    files = glob.glob(filepath, recursive=False)
    key_set = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        # x=len(files)
        # Upload the file
        for i, file in enumerate(files):
            worker = Boto3_Worker(file, bucket, root_prefix)
            futures.add(executor.submit(worker.upload_s3, s3_client))

            last_file = (i == (len(files) - 1))

            # if (x - 1) % max_workers == 0: # run max workers until they complete and start over
            if len(futures) >= max_workers or last_file: # keep max workers busy

                # done, not_done = wait(futures, return_when=ALL_COMPLETED)
                done, not_done = wait(
                        futures,
                        return_when=ALL_COMPLETED if last_file else FIRST_COMPLETED
                    )
                for d in done:
                    logger.info(d.result())
                    stat, key_path, meta = d.result()
                    logger.info(f"key path: {key_path} {meta}")
                    key_set.add(os.path.dirname(key_path))
                # for nd in not_done:
                #     logger.info(nd.result())
                # logger.info("next batch....")
            #     futures.clear()
            # x-=1

    logger.info(key_set)
    # iterate over the set of keys for fix posix permissions
    for key in key_set:
        dir_path = Path(key)
        logger.info(f"confirm this path {dir_path}/")
        status = check_and_update(f"{str(dir_path)}/", bucket, s3_client)
        if not status:
            break # no need to recurse further back up the tree as last dir already has permissions
        dir_path = dir_path.parent
