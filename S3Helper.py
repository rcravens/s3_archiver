import hashlib
import os
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool

import boto3
from botocore.exceptions import ClientError


class S3Helper:
    def __init__(self, bucket_name, aws_access_key, aws_secret_key):
        self.bucket_name = bucket_name
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.directory_map = {}

    def add_directory_map(self, local_directory: str, s3_key: str) -> None:
        """
        This function and the next allow you to map a local directory to an s3 bucket prefix.
        For example:
        /Users/rcravens/Google Drive/My Drive/__youtube ====> youtube
        /Users/rcravens/Dropbox/_youtube                ====> youtube
        """
        self.directory_map[local_directory] = s3_key

    def get_s3_name(self, file_path: str) -> str:
        path_parts = file_path.split(os.sep)
        found_key = None
        search_key = '/'
        for part in path_parts:
            search_key = os.path.join(search_key, part)
            if search_key in self.directory_map:
                found_key = search_key

        if found_key is None:
            return file_path

        s3_base_key = self.directory_map[found_key]
        return file_path.replace(found_key, s3_base_key)

    def archive_directory(self, local_directory: str, is_incremental: bool = True):
        changed_files = self.find_changed_files(local_directory)

        files_to_upload = []
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)

                if not is_incremental or local_path in changed_files:
                    files_to_upload.append(local_path)

        pool = ThreadPool(processes=5)
        pool.map(self.archive_file, files_to_upload)

    def archive_file(self, local_path: str):
        s3_name = self.get_s3_name(local_path)

        try:
            s3 = self._get_s3_client()
            with open(local_path, 'rb') as f:
                s3.upload_fileobj(f, self.bucket_name, s3_name)
        except (ClientError, FileNotFoundError) as e:
            print(e)

    def delete_archive(self, local_directory: str):
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                self.delete_file(local_path)

    def delete_file(self, local_path: str):
        s3_name = self.get_s3_name(local_path)

        try:
            s3 = self._get_s3_client()
            s3.delete_object(Bucket=self.bucket_name, Key=s3_name)
        except (ClientError, FileNotFoundError) as e:
            print(e)

    def is_archive_valid(self, local_directory: str, use_e_tag: bool = False) -> bool:
        changed_files = self.find_changed_files(local_directory, use_e_tag=use_e_tag)

        return len(changed_files) == 0

    def find_changed_files(self, local_directory: str, use_e_tag: bool = False) -> list:
        differences = []

        s3_prefix = self.get_s3_name(local_directory)

        s3_object_lut = self._get_s3_objects(s3_prefix)

        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)

                s3_name = self.get_s3_name(local_path)

                if s3_name in s3_object_lut:
                    s3_obj = s3_object_lut[s3_name]
                    s3_size = s3_obj.size
                    s3_last_modified = s3_obj.last_modified

                    if use_e_tag:
                        e_tag = s3_obj.e_tag[1:-1]
                        local_e_tag = self._calculate_local_e_tag(local_path, e_tag)

                        if e_tag != local_e_tag:
                            differences.append(local_path)
                    else:
                        stat_obj = os.stat(local_path)
                        file_size = stat_obj.st_size
                        last_modified = stat_obj.st_mtime
                        modified = datetime.fromtimestamp(last_modified, tz=timezone.utc)

                        if file_size != s3_size:
                            differences.append(local_path)

                        if modified > s3_last_modified:
                            differences.append(local_path)
                else:
                    differences.append(local_path)

        return differences

    def _get_s3_client(self) -> boto3.client:
        s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
        )
        return s3

    def _get_s3_objects(self, prefix: str) -> dict:
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
        )

        s3 = session.resource('s3')

        bucket = s3.Bucket(self.bucket_name)

        objs = list(bucket.objects.filter(Prefix=prefix))

        result = {}
        for obj in objs:
            result[obj.key] = obj

        return result

    @staticmethod
    def _calculate_local_e_tag(local_path: str, e_tag: str) -> str:
        if '-' not in e_tag:
            return hashlib.md5(open(local_path, 'rb').read()).hexdigest()
        else:
            boto3_part_size = 8388608

            md5_digests = []
            with open(local_path, 'rb') as f:
                for chunk in iter(lambda: f.read(boto3_part_size), b''):
                    md5_digests.append(hashlib.md5(chunk).digest())

            local_e_tag = hashlib.md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))

            return local_e_tag
