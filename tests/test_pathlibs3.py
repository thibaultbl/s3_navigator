import pytest
import boto3
from pathlibs3.pathlibs3 import S3Path, upload_file
from pathlib import Path
import datetime
from datetime import timezone


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

        new_path = navigator / "test/random"
        assert new_path.path == "test/random"

    def test_list_folder(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="")
        res = navigator.iterdir()

        assert list(res) == [
            S3Path(client, bucket, "folder1/"),
            S3Path(client, bucket, "folder2/"),
        ]

    def test_list_folder_without_ending_slash(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2/")
        res = navigator.iterdir()

        navigator = S3Path(client, bucket=bucket, path="folder2")
        res_without_slash = navigator.iterdir()

        assert (
            [x.path for x in res]
            == [x.path for x in res_without_slash]
            == ["folder2/folder1-1/", "folder2/test3.txt"]
        )

    def test_list_folder_recursively_without_ending_slash(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2/")
        res = navigator.iterdir(recursive=True)

        navigator = S3Path(client, bucket=bucket, path="folder2")
        res_without_slash = navigator.iterdir(recursive=True)

        assert (
            [x.path for x in res]
            == [x.path for x in res_without_slash]
            == [
                "folder2/folder1-1/",
                "folder2/folder1-1/test2.txt",
                "folder2/test3.txt",
            ]
        )

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

    def test_parents(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/folder2/test.txt")

        parents = navigator.parents
        assert len(parents) == 2
        assert parents[0] == S3Path(client, bucket=bucket, path="folder1/folder2")
        assert parents[1] == S3Path(client, bucket=bucket, path="folder1")

    def test_name(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/folder2/test.txt")

        assert navigator.name == "test.txt"

    def test_stem(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/folder2/test.tar.gz")

        assert navigator.stem == "test.tar"

    def test_exists(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")

        # Check for file
        assert navigator.exists() == True
        navigator = S3Path(client, bucket=bucket, path="folder1/filedonotexists.txt")

        assert navigator.exists() == False

        # Check for folder
        existing_folder = S3Path(client, bucket=bucket, path="folder1/")
        assert existing_folder.exists() == True

        existing_folder = S3Path(client, bucket=bucket, path="folder1")
        assert existing_folder.exists() == True

        existing_folder = S3Path(client, bucket=bucket, path="folder999")
        assert existing_folder.exists() == False

        existing_folder = S3Path(client, bucket=bucket, path="folder999/")
        assert existing_folder.exists() == False

    @pytest.mark.parametrize("is_str", [True, False])
    def test_copy_file(self, setup_bucket, bucket, tmp_path, is_str):
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
        S3Path.copy(new_file if not is_str else str(new_file), new_s3_file)
        assert new_s3_file.exists() == True

        # From local to s3
        new_file = tmp_path / "local_new_file.txt"
        assert new_file.exists() == False
        S3Path.copy(navigator, new_file if not is_str else str(new_file))
        assert new_file.exists() == True

    @pytest.mark.parametrize("is_str", [True, False])
    def test_copy_folder(self, setup_bucket, bucket, tmp_path, is_str):
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
        S3Path.copy(tmp_path if not is_str else str(tmp_path), new_s3_file)

        new_s3_file = S3Path(client, bucket=bucket, path="folder5/new_file.txt")
        assert new_s3_file.exists() == True

        # From s3 to local
        new_file = tmp_path / "folder6"
        new_file.mkdir()
        navigator = S3Path(client, bucket=bucket, path="folder2/")
        S3Path.copy(navigator, new_file if not is_str else str(new_file))
        assert set([x.name for x in new_file.iterdir()]) == {
            "folder1-1",
            "test3.txt",
        }

    @pytest.mark.parametrize("is_str", [True, False])
    def test_copy_folder_without_ending_slash(
        self, setup_bucket, bucket, tmp_path, is_str
    ):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2")

        # From s3 to s3
        new_file = S3Path(client, bucket=bucket, path="folder4")
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
        S3Path.copy(tmp_path if not is_str else str(tmp_path), new_s3_file)

        new_s3_file = S3Path(client, bucket=bucket, path="folder5/new_file.txt")
        assert new_s3_file.exists() == True

        # From s3 to local
        new_file = tmp_path / "folder6"
        new_file.mkdir()
        navigator = S3Path(client, bucket=bucket, path="folder2")
        S3Path.copy(navigator, new_file if not is_str else str(new_file))
        assert set([x.name for x in new_file.iterdir()]) == {
            "folder1-1",
            "test3.txt",
        }

    def test_last_modified(self, setup_bucket, bucket):
        client = setup_bucket

        # With a file
        navigator = S3Path(client, bucket=bucket, path="folder1/test.txt")

        assert navigator.last_modified.date() == datetime.datetime.now().date()

        # with a folder
        navigator = S3Path(client, bucket=bucket, path="folder2")

        assert navigator.last_modified.date() == datetime.datetime.now().date()


    def test_delete(self, setup_bucket, bucket):
        client = setup_bucket
        navigator = S3Path(client, bucket=bucket, path="folder2/")

        objects_before = [x for x in navigator.iterdir(recursive=True)]

        navigator.delete()

        objects_after = [x for x in navigator.iterdir(recursive=True)]

        assert len(objects_before) == 3
        assert len(objects_after) == 0

    def test_move(self, setup_bucket, bucket):
        client = setup_bucket
        source_folder = S3Path(client, bucket=bucket, path="folder2/")
        destination_folder = S3Path(client, bucket=bucket, path="new_folder/subfolder/")

        objects_before = [x for x in source_folder.iterdir(recursive=True)]
        destination_folder_before = [
            x for x in destination_folder.iterdir(recursive=True)
        ]

        S3Path.move(source_folder, destination_folder)

        objects_after = [x for x in source_folder.iterdir(recursive=True)]
        destination_folder_after = [
            x for x in destination_folder.iterdir(recursive=True)
        ]

        assert len(objects_before) == 3
        assert len(objects_after) == 0

        assert len(destination_folder_before) == 0
        assert len(destination_folder_after) == 3

        contents_before = [
            x.path.replace(source_folder.path, "") for x in objects_before
        ]
        contents_after = [
            x.path.replace(destination_folder.path, "")
            for x in destination_folder_after
        ]
        assert contents_before == contents_after
