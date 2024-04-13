import boto3

class S3Navigator:
    def __init__(self, client: boto3.client, bucket: str):
        self.client = client
        self.bucket = bucket
    
    def list_folder(self, folder: str):
        sub_folders = list()

        result = self.client.list_objects(Bucket=self.bucket, Prefix=folder, Delimiter='/')
        for o in result.get('CommonPrefixes'):
            sub_folders.append(o.get('Prefix'))
        return sub_folders