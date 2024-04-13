import boto3
from s3_navigator.s3_navigator import S3Navigator

class TestS3Navigator:
    def test_list_folder(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Navigator(client, bucket=bucket)
        res = navigator.list_folder("")

        assert res == ["folder1/"]

