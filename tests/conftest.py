import boto3
import pytest 

@pytest.fixture(scope="function")
def aws(aws_credentials):
    with boto3.mock_aws():
        yield boto3.client("s3", region_name="us-east-1")
 

@pytest.fixture
def create_bucket1(aws):
    boto3.client("s3").create_bucket(Bucket="b1")