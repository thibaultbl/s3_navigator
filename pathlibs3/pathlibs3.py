import boto3
import logging
from pathlib import Path
import boto3.exceptions
import botocore
from typing import Union
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)


def upload_file(
    client, source, destination_bucket, destination_path, exists_ok: bool = False
):
    try:
        exist = True
        if not exists_ok:
            try:
                client.head_object(Bucket=destination_bucket, Key=destination_path)
            except botocore.exceptions.ClientError:
                exist = False
        if exists_ok or exist is False:
            client.upload_file(str(source), destination_bucket, destination_path)
    except ClientError as e:
        if e.response["Error"]["Code"] == "FileExists":
            logging.error(f"File {destination_path} already exist")
        else:
            raise e


class S3Path:
    def __init__(self, client: boto3.client, bucket: str, path: Union[str, Path]):
        self.client = client
        self.bucket = bucket
        self.path = str(path)

    @property
    def path_dir(self):
        if self.is_dir() and not self.path.endswith("/") and self.path != "":
            return self.path + "/"

        return self.path

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
        logging.debug("looking for folder %s", self.path)
        sub_folders = list()

        result = self.client.list_objects(
            Bucket=self.bucket, Prefix=self.path_dir, Delimiter="/"
        )
        for object in self._retrieve_folder_contents(result):
            object = S3Path(self.client, self.bucket, object)
            if object.path == self.path:
                continue
            sub_folders.append(object)
            if object.is_dir() and recursive:
                subfolder = object.iterdir(recursive=recursive)
                sub_folders += subfolder

        if only_files:
            sub_folders = [x for x in sub_folders if not x.is_dir()]

        for subfolder in sub_folders:
            yield subfolder

    def is_dir(self) -> bool:
        return self._is_dir()

    def _is_dir(self) -> bool:
        if self.path == "":
            return True

        try:
            result = self.client.head_object(Bucket=self.bucket, Key=self.path)
        except ClientError:
            return True

        return False

    def exists(self):
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.path)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    @property
    def name(self):
        return Path(self.path).name

    @property
    def stem(self):
        return Path(self.path).stem

    def __truediv__(self, other: str) -> "S3Path":
        return S3Path(
            client=self.client,
            bucket=self.bucket,
            path=f"{self.path}/{other}".replace("//", "/").lstrip("/"),
        )

    @classmethod
    def _copy_from_s3_to_s3(cls, source: "S3Path", destination: "S3Path"):
        logging.info("Copying from s3 to s3: %s to %s", source, destination)
        client = source.client
        copy_source = {"Bucket": source.bucket, "Key": source.path}
        client.copy(copy_source, destination.bucket, destination.path)

    @classmethod
    def _copy_from_local_to_s3(cls, source: Path, destination: "S3Path"):
        logging.info("Copying from local to s3: %s to %s", source, destination)
        client = destination.client
        upload_file(client, str(source), destination.bucket, destination.path)

    @classmethod
    def _copy_from_s3_to_local(cls, source: "S3Path", destination: Path):
        logging.info("Copying from s3 to local: %s to %s", source, destination)
        client = source.client
        with open(str(destination), "wb") as f:
            client.download_fileobj(source.bucket, source.path, f)

    @classmethod
    def copy(
        cls, origin: Union["S3Path", Path, str], destination: Union["S3Path", Path, str]
    ):
        if isinstance(origin, str):
            origin = Path(origin)

        if isinstance(destination, str):
            destination = Path(destination)

        if origin.is_dir():
            logging.info("%s is a directory", origin)
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

    @property
    def parents(self):
        return [
            S3Path(self.client, self.bucket, parent)
            for parent in Path(self.path).parents
            if str(parent) != "."
        ]

    def delete(self):
        objects = self.iterdir(recursive=True, only_files=True)

        for object in objects:
            object.client.delete_object(Bucket=object.bucket, Key=object.path)

    @classmethod
    def move(cls, source: "S3Path", destination: "S3Path"):
        objects = source.iterdir(recursive=True, only_files=True)

        for object in objects:
            copy_source = {"Bucket": object.bucket, "Key": object.path}

            destination_path = object.path.replace(source.path, "")

            object.client.copy_object(
                CopySource=copy_source,
                Bucket=destination.bucket,
                Key=f"{destination.path}{destination_path}",
            )
            object.client.delete_object(Bucket=object.bucket, Key=object.path)
