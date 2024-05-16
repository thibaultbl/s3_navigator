import boto3
import logging
from pathlib import Path
import botocore
from typing import Union


class S3Path:
    def __init__(self, client: boto3.client, bucket: str, path: str):
        self.client = client
        self.bucket = bucket
        self.path = path

    def __repr__(self):
        return f"S3Path(bucket={self.bucket}, path={self.path})"

    def __eq__(self, other) -> bool:
        return (
            self.path == other.path
            and self.client == other.client
            and self.bucket == other.bucket
        )

    def __str__(self):
        return self.path

    def _retrieve_folder_contents(self, results):
        contents = []
        if "CommonPrefixes" in results.keys():
            contents += [x["Prefix"] for x in results.get("CommonPrefixes")]
        if "Contents" in results.keys():
            contents += [x["Key"] for x in results.get("Contents")]

        return contents

    def iterdir(self, recursive: bool = False, only_files: bool = False):
        logging.warning("looking for folder %s", self.path)
        sub_folders = list()

        result = self.client.list_objects(
            Bucket=self.bucket, Prefix=self.path, Delimiter="/"
        )
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

    def exists(self):
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.path)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    @property
    def name(self):
        return Path(self.path).name

    def __truediv__(self, other: str) -> "S3Path":
        return S3Path(
            client=self.client,
            bucket=self.bucket,
            path=str(Path(self.path) / other),
        )

    @classmethod
    def _copy_from_s3_to_s3(cls, source: "S3Path", destination: "S3Path"):
        client = source.client
        copy_source = {"Bucket": source.bucket, "Key": source.path}
        client.copy(copy_source, destination.bucket, destination.path)

    @classmethod
    def _copy_from_local_to_s3(cls, source: Path, destination: "S3Path"):
        client = destination.client
        client.upload_file(str(source), destination.bucket, destination.path)

    @classmethod
    def _copy_from_s3_to_local(cls, source: "S3Path", destination: Path):
        client = source.client
        with open(str(destination), "wb") as f:
            client.download_fileobj(source.bucket, source.path, f)

    @classmethod
    def copy(cls, origin: Union["S3Path", Path], destination: Union["S3Path", Path]):
        if origin.is_dir():
            for path in origin.iterdir():
                cls.copy(path, destination / path.name)

        else:
            if isinstance(origin, S3Path) and isinstance(destination, S3Path):
                cls._copy_from_s3_to_s3(origin, destination)

            if isinstance(origin, Path) and isinstance(destination, S3Path):
                cls._copy_from_local_to_s3(origin, destination)

            if isinstance(origin, S3Path) and isinstance(destination, Path):
                destination.parent.mkdir(parents=True, exist_ok=True)
                cls._copy_from_s3_to_local(origin, destination)

    @property
    def parent(self):
        return S3Path(self.client, self.bucket, str(Path(self.path).parent))
