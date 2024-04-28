import boto3
import logging
from pathlib import Path


class S3Path:
    def __init__(self, client: boto3.client, bucket: str, path: str):
        self.client = client
        self.bucket = bucket
        self.path = path
    
    def __eq__(self, other) -> bool:
        return self.path == other.path and self.client == other.client and self.bucket == other.bucket
    
    def __str__(self):
        return self.path
    
    def _retrieve_folder_contents(self, results):
        contents = []
        if 'CommonPrefixes' in results.keys():
            contents +=  [x["Prefix"] for x in results.get('CommonPrefixes')]
        if 'Contents' in results.keys():
             contents += [x["Key"] for x in results.get("Contents")]
    
        return contents
    
    def iterdir(self, recursive: bool=False, only_files: bool=False):
        logging.warning("looking for folder %s", self.path)
        sub_folders = list()

        result = self.client.list_objects(Bucket=self.bucket, Prefix=self.path, Delimiter='/')
        for object in self._retrieve_folder_contents(result):
            object = S3Path(self.client, self.bucket, object)
            sub_folders.append(object)
            if object.is_dir() and recursive:
                subfolder = object.iterdir()
                sub_folders += subfolder

        if only_files:
            sub_folders = [x for x in sub_folders if not x.is_dir()]

        for subfolder in sub_folders:
            yield subfolder
        
    def is_dir(self):
        return self.path.endswith("/")

    @property
    def parent(self):
        return S3Path(self.client, self.bucket, str(Path(self.path).parent))