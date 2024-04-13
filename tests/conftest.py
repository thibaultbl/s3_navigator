import boto3
import pytest 
import moto
import os

@pytest.fixture
def bucket():
    return "bucket1"

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def setup_bucket(tmp_path, bucket):
    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket)
        
        with open(tmp_path / "demofile2.txt", "a") as f:
            f.write("Now the file has more content!")
            f.close()

        response = client.upload_file(tmp_path / "demofile2.txt", bucket, "folder1/test.txt")

        yield client
