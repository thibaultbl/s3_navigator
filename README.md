# Installation

```sh
pip install pathlibs3
```

# Usage

## Create a PathlibS3 Object
```python
from pathlibs3.pathlibs3 import S3Path
# Create a pathlibs3

client = boto3.client("s3", region_name="us-east-1")
bucket = "test-bucket"

# Create an object to s3://test-bucket/myfolder
s3_path_to_myfolder = S3Path(client, bucket, "myfolder/")


# You can also concatenate object
# Create an object to s3://test-bucket/myfile/random_file.txt
s3_path_to_random_file = s3_path_to_myfolder / "random_file.txt"
```

## Iter over a directory

```Python
# Iter over this directory
for path in s3_path.iterdir():
    print(path)

# Iter over this directory recursively
for path in s3_path.iterdir(recursive=True):
    print(path)
```

## Copy file or folder

### Copy from s3 to local
```python
# Create an pathlibs3 object
s3_path_to_myfolder = S3Path(client, bucket, "myfolder/")

# Create a pathlib object
local_path = Path("/tmp/local_folder")

# Will download the s3 folder localy
S3Path.copy(s3_path_to_myfolder, local_path)
```

### Copy from local to s3
```python
# Create an pathlibs3 object
s3_path_to_myfolder = S3Path(client, bucket, "myfolder/")

# Create a pathlib object
local_path = Path("/tmp/local_folder")

# Will download the s3 folder localy
S3Path.copy(local_path, s3_path_to_myfolder)

```


### Copy from s3 to s3
```python
# Create an pathlibs3 object
s3_path_to_myfolder = S3Path(client, bucket, "myfolder/")

# Create another pathlibs3 object
s3_path_to_anotherfolder = S3Path(client, bucket, "anotherfolder/")

# Will download the s3 folder localy
S3Path.copy(s3_path_to_myfolder, s3_path_to_anotherfolder)
```

# Contribution
## run test

run test with `poetry run python -m pytest`
