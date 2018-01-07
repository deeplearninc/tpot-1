import os
import os.path
import boto3
try:
  from urllib.parse import urlparse
except ImportError:
  from urlparse import urlparse

#!! This file is duplicated in TPOT and AugerML. Modify it in AugerML and copy here!! 

class AugerS3FSClient:

    def __init__(self, path):
        self.bucket = os.environ.get('S3_DATA_PATH')

        if path is not None and (path.startswith("s3://") or path.startswith("s3a://") or path.startswith("s3n://")):
            uri = urlparse(path)
            self.bucket = uri.netloc

        boto3.setup_default_session(aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
        self.client = boto3.client('s3')

    def write_text_file(self, path, data):
        bucket, key = self._get_bucket_and_key(path)
        self.client.put_object(Body=data, Bucket=bucket, Key=key)

    def read_text_file(self, path):
        bucket, key = self._get_bucket_and_key(path)
        obj = self.client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')

    def rename_file(self, old_path, new_path):
        old_bucket, old_key = self._get_bucket_and_key(old_path)
        new_bucket, new_key = self._get_bucket_and_key(new_path)

        self.client.copy_object(Bucket=new_bucket, Key=new_key, CopySource={'Bucket': old_bucket, 'Key': old_key})
        self.client.delete_object(Bucket=old_bucket, Key=old_key)

    def delete_file(self, path):
        bucket, key = self._get_bucket_and_key(path)
        self.client.delete_object(Bucket=bucket, Key=key)

    def is_file_exists(self, path):
        bucket, key = self._get_bucket_and_key(path)
        exists = False
        try:
            res = self.client.head_object(Bucket=bucket, Key=key)
            exists = True
        except Exception as e:
            pass

        return exists

    def _get_bucket_and_key(self, path):
        if path.startswith("s3://") or path.startswith("s3a://") or path.startswith("s3n://"):
            uri = urlparse(path)
            bucket, key = uri.netloc, uri.path[1:]
        else:
            bucket, key = self.bucket, path
                
        return bucket, key.replace("//", "/")
            
class AugerLocalFSClient:

    def create_path(self, path):
        try:
            os.makedirs(path)
        except OSError:
            pass

    def write_text_file(self, path, data):
        self.create_path(os.path.dirname(path))
        with open(path, "w+") as file:
          return file.write(data)

    def read_text_file(self, path):
        with open(path, "r") as file:
          return file.read()

    def rename_file(self, old_path, new_path):
        os.rename(old_path, new_path )

    def delete_file(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

    def is_file_exists(self, path):
        return os.path.isfile(path)

class AugerFSClient:
    def __init__(self, root_path = None):
        self.fs_client = None
        self.root_path = root_path

        if self.root_path:
            self.fs_client = self._make_fs_client(self.root_path)
            
    def write_text_file(self, path, data):
        self.fs_client = self._make_fs_client(path)
        if self.root_path:
            path = os.path.join(self.root_path, path)

        self.fs_client.write_text_file(path, data)

    def read_text_file(self, path):
        self.fs_client = self._make_fs_client(path)
        if self.root_path:
            path = os.path.join(self.root_path, path)

        return self.fs_client.read_text_file(path)

    def rename_file(self, old_path, new_path):
        self.fs_client = self._make_fs_client(old_path)
        if self.root_path:
            old_path = os.path.join(self.root_path, old_path)
            new_path = os.path.join(self.root_path, new_path)

        self.fs_client.rename_file(old_path, new_path)

    def delete_file(self, path):
        self.fs_client = self._make_fs_client(path)
        if self.root_path:
            path = os.path.join(self.root_path, path)

        self.fs_client.delete_file(path)

    def write_json_file(self, path, data):
        import json
        self.write_text_file(path, json.dumps(data))

    def read_json_file(self, path):
        import json
        return json.loads(self.read_text_file(path))

    def is_file_exists(self, path):
        self.fs_client = self._make_fs_client(path)
        if self.root_path:
            path = os.path.join(self.root_path, path)

        return self.fs_client.is_file_exists(path)
            
    def _make_fs_client(self, path):
        if self.fs_client is None:
            if path.startswith("s3"):
                self.fs_client = AugerS3FSClient(path)
            else:
                self.fs_client = AugerLocalFSClient()

        return self.fs_client


