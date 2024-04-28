import boto3
import logging

def is_directory(object: str) -> bool:
    return object.endswith("/")

class S3Navigator:
    def __init__(self, client: boto3.client, bucket: str):
        self.client = client
        self.bucket = bucket
    
    def _retrieve_folder_contents(self, results):
        contents = []
        if 'CommonPrefixes' in results.keys():
            contents +=  [x["Prefix"] for x in results.get('CommonPrefixes')]
        if 'Contents' in results.keys():
             contents += [x["Key"] for x in results.get("Contents")]
    
        return contents
    
    def list_folder(self, folder: str, recursive: bool=False, only_files: bool=False):
        logging.warning("looking for folder %s", folder)
        sub_folders = list()

        result = self.client.list_objects(Bucket=self.bucket, Prefix=folder, Delimiter='/')
        for object in self._retrieve_folder_contents(result):
            sub_folders.append(object)
            if is_directory(object) and recursive:
                subfolder = self.list_folder(object)
                sub_folders += subfolder
        
        if only_files:
            sub_folders = [x for x in sub_folders if not is_directory(x)]
        return sub_folders