import boto3
from s3_navigator.s3_navigator import S3Navigator

class TestS3Navigator:
    def test_list_folder(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Navigator(client, bucket=bucket)
        res = navigator.list_folder("")

        assert res == ["folder1/", "folder2/"]
    
    def test_list_folder_recursively(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Navigator(client, bucket=bucket)
        res = navigator.list_folder("", recursive=True)

        assert res == ['folder1/', 'folder1/test.txt', 'folder2/', 'folder2/folder1-1/', 'folder2/test3.txt']
    
    def test_list_folder_recursively_only_files(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Navigator(client, bucket=bucket)
        res = navigator.list_folder("", recursive=True, only_files=True)

        assert res == ['folder1/test.txt', 'folder2/test3.txt']
    

