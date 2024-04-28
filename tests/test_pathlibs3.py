import boto3
from pathlibs3.pathlibs3 import S3Path

class TestS3Path:
    def test_list_folder(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir()

        assert list(res) == [S3Path(client, bucket, "folder1/"), S3Path(client, bucket, "folder2/")]
    
    def test_list_folder_recursively(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir(recursive=True)

        assert list(res) == [S3Path(client, bucket, x) for x in ['folder1/', 'folder1/test.txt', 'folder2/', 'folder2/folder1-1/', 'folder2/test3.txt']]
    
    def test_list_folder_recursively_only_files(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir(recursive=True, only_files=True)

        assert list(res) == [S3Path(client, bucket, x) for x in ['folder1/test.txt', 'folder2/test3.txt']]
    
    def test_is_dir(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path='folder1/')

        assert navigator.is_dir() == True

        navigator = S3Path(client, bucket=bucket, path='folder1/test.txt')

        assert navigator.is_dir() == False


    

