import boto3
from pathlibs3.pathlibs3 import S3Path, upload_file
from pathlib import Path


def test_upload_file(setup_bucket, bucket, tmp_path):
    client = setup_bucket

    file = "folder1/test.txt"
    file_s3 = S3Path(client, bucket, file)

    # Contents before upload
    local_file = Path(tmp_path / "existing.txt")
    S3Path.copy(file_s3, local_file)

    with open(tmp_path / "existing.txt", "r") as f:
        res = f.read()
        assert res == "Now the file has more content!"

    new_file = tmp_path / "new_file.txt"
    with open(new_file, "a") as f:
        f.write("New contents!")
        f.close()

    # Without exists ok
    upload_file(client, new_file, bucket, file)

    S3Path.copy(file_s3, local_file)

    with open(tmp_path / "existing.txt", "r") as f:
        res = f.read()
        assert res == "Now the file has more content!"

    # With exists ok
    upload_file(client, new_file, bucket, file, exists_ok=True)

    S3Path.copy(file_s3, local_file)

    with open(tmp_path / "existing.txt", "r") as f:
        res = f.read()
        assert res == "New contents!"


class TestS3Path:
    def test_concatenation(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")

        assert navigator.path == ""

        new_path = navigator / "test/test.txt"
        assert new_path.path == "test/test.txt"

        new_path = navigator / "test/random/"
        assert new_path.path == "test/random/"

    def test_list_folder(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir()

        assert list(res) == [
            S3Path(client, bucket, "folder1/"),
            S3Path(client, bucket, "folder2/"),
        ]

    def test_list_folder_recursively(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir(recursive=True)

        assert set([x.path for x in res]) == set(
            [
                "folder1/",
                "folder1/test.txt",
                "folder2/",
                "folder2/folder1-1/",
                "folder2/test3.txt",
                "folder2/folder1-1/test2.txt",
            ]
        )

    def test_list_folder_recursively_with_slash(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2/")
        res = navigator.iterdir(recursive=True)

        assert set([x.path for x in res]) == {
            "folder2/test3.txt",
            "folder2/folder1-1/test2.txt",
            "folder2/folder1-1/",
        }

    def test_list_folder_recursively_only_files(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir(recursive=True, only_files=True)

        assert set([x.path for x in res]) == set(
            ["folder1/test.txt", "folder2/test3.txt", "folder2/folder1-1/test2.txt"]
        )

    def test_is_dir(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/")

        assert navigator.is_dir() == True

        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")

        assert navigator.is_dir() == False

        navigator = S3Path(client, bucket=bucket, path="folder1")

        assert navigator.is_dir() == True

    def test_parent(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")
        assert navigator.parent == S3Path(client, bucket=bucket, path="folder1")

    def test_exists(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")

        assert navigator.exists() == True
        navigator = S3Path(client, bucket=bucket, path="folder1/filedonotexists.txt")

        assert navigator.exists() == False

    def test_copy_file(self, setup_bucket, bucket, tmp_path):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")

        # From s3 to s3
        new_file = S3Path(client, bucket=bucket, path="folder2/test.txt")
        assert new_file.exists() == False
        S3Path.copy(navigator, new_file)
        assert new_file.exists() == True

        # From s3 to local
        with open(tmp_path / "new_file.txt", "a") as f:
            f.write("Now the file has more content!")
            f.close()

        new_file = tmp_path / "new_file.txt"
        new_s3_file = S3Path(client, bucket=bucket, path="folder4/new_file.txt")
        assert new_file.exists() == True
        assert new_s3_file.exists() == False
        S3Path.copy(new_file, new_s3_file)
        assert new_s3_file.exists() == True

        # From local to s3
        new_file = tmp_path / "local_new_file.txt"
        assert new_file.exists() == False
        S3Path.copy(navigator, new_file)
        assert new_file.exists() == True

    def test_copy_folder(self, setup_bucket, bucket, tmp_path):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2/")

        # From s3 to s3
        new_file = S3Path(client, bucket=bucket, path="folder4/")
        assert new_file.exists() == False
        S3Path.copy(navigator, new_file)

        assert [x.path for x in new_file.iterdir(recursive=True)] == [
            "folder4/folder1-1/",
            "folder4/folder1-1/test2.txt",
            "folder4/test3.txt",
        ]
        assert (
            S3Path(client, bucket=bucket, path="folder4/folder1-1/test2.txt").exists()
            == True
        )

        # From local to s3
        with open(tmp_path / "new_file.txt", "a") as f:
            f.write("Now the file has more content!")
            f.close()

        new_file = tmp_path / "new_file.txt"
        new_s3_file = S3Path(client, bucket=bucket, path="folder5")
        assert new_s3_file.exists() == False
        S3Path.copy(tmp_path, new_s3_file)

        new_s3_file = S3Path(client, bucket=bucket, path="folder5/new_file.txt")
        assert new_s3_file.exists() == True

        # From s3 to local
        new_file = tmp_path / "folder6"
        new_file.mkdir()
        navigator = S3Path(client, bucket=bucket, path="folder2/")
        S3Path.copy(navigator, new_file)
        assert set([x.name for x in new_file.iterdir()]) == {
            "folder1-1",
            "test3.txt",
        }
