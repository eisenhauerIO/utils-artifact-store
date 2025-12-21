"""Tests for ArtifactStore - local storage operations."""

import json
import os
import shutil
import tempfile

import pandas as pd
import pytest
import yaml

from artifact_store import ArtifactStore, ArtifactStoreError, MissingDependencyError, StorageError


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def store(temp_dir):
    """Create an ArtifactStore with a temp directory."""
    return ArtifactStore(temp_dir)


class TestArtifactStoreInit:
    """Test ArtifactStore initialization."""

    def test_absolute_path(self, temp_dir):
        """Test initialization with absolute path."""
        store = ArtifactStore(temp_dir)
        assert store.base_path == temp_dir
        assert not store.is_s3

    def test_relative_path(self):
        """Test initialization with relative path."""
        store = ArtifactStore("output")
        assert os.path.isabs(store.base_path)
        assert store.base_path.endswith("output")
        assert not store.is_s3

    def test_s3_path(self):
        """Test initialization with S3 URI."""
        store = ArtifactStore("s3://my-bucket/prefix")
        assert store.base_path == "s3://my-bucket/prefix"
        assert store.is_s3

    def test_s3_path_strips_trailing_slash(self):
        """Test that trailing slash is stripped from S3 path."""
        store = ArtifactStore("s3://my-bucket/prefix/")
        assert store.base_path == "s3://my-bucket/prefix"


class TestFullPath:
    """Test full_path method."""

    def test_full_path_local(self, store):
        """Test full path for local storage."""
        path = store.full_path("subdir/file.csv")
        assert path == os.path.join(store.base_path, "subdir", "file.csv")

    def test_full_path_empty(self, store):
        """Test full path with empty relative path."""
        path = store.full_path("")
        assert path == store.base_path

    def test_full_path_strips_leading_slash(self, store):
        """Test that leading slash is stripped."""
        path1 = store.full_path("file.csv")
        path2 = store.full_path("/file.csv")
        assert path1 == path2


class TestFromFilePath:
    """Test from_file_path class method."""

    def test_from_local_file_path(self):
        """Test creating store from local file path."""
        store, filename = ArtifactStore.from_file_path("/path/to/file.csv")
        assert store.base_path == "/path/to"
        assert filename == "file.csv"

    def test_from_s3_file_path(self):
        """Test creating store from S3 file path."""
        store, filename = ArtifactStore.from_file_path("s3://bucket/path/to/file.csv")
        assert store.base_path == "s3://bucket/path/to"
        assert filename == "file.csv"
        assert store.is_s3


class TestTextOperations:
    """Test text read/write operations."""

    def test_write_and_read_text(self, store):
        """Test writing and reading text."""
        content = "Hello, World!"
        store.write_text("test.txt", content)
        result = store.read_text("test.txt")
        assert result == content

    def test_write_text_creates_dirs(self, store):
        """Test that write_text creates parent directories."""
        store.write_text("subdir/nested/test.txt", "content")
        assert os.path.exists(store.full_path("subdir/nested/test.txt"))


class TestBytesOperations:
    """Test bytes read/write operations."""

    def test_write_and_read_bytes(self, store):
        """Test writing and reading bytes."""
        content = b"\x00\x01\x02\x03"
        store.write_bytes("test.bin", content)
        result = store.read_bytes("test.bin")
        assert result == content


class TestCSVOperations:
    """Test CSV read/write operations."""

    def test_write_and_read_csv(self, store):
        """Test writing and reading CSV."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        store.write_csv("test.csv", df)
        result = store.read_csv("test.csv")
        pd.testing.assert_frame_equal(result, df)

    def test_read_csv_directory(self, store):
        """Test reading multiple CSVs from directory."""
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"a": [3, 4]})

        os.makedirs(store.full_path("data"), exist_ok=True)
        store.write_csv("data/file1.csv", df1)
        store.write_csv("data/file2.csv", df2)

        result = store.read_csv("data")
        assert len(result) == 4
        assert list(result["a"]) == [1, 2, 3, 4] or list(result["a"]) == [3, 4, 1, 2]

    def test_read_csv_not_found(self, store):
        """Test reading non-existent CSV."""
        with pytest.raises(FileNotFoundError):
            store.read_csv("nonexistent.csv")


class TestJSONOperations:
    """Test JSON read/write operations."""

    def test_write_and_read_json(self, store):
        """Test writing and reading JSON."""
        data = {"key": "value", "number": 42, "nested": {"a": 1}}
        store.write_json("test.json", data)
        result = store.read_json("test.json")
        assert result == data


class TestYAMLOperations:
    """Test YAML read/write operations."""

    def test_write_and_read_yaml(self, store):
        """Test writing and reading YAML."""
        data = {"key": "value", "list": [1, 2, 3]}
        store.write_yaml("test.yaml", data)
        result = store.read_yaml("test.yaml")
        assert result == data

    def test_read_invalid_yaml(self, store):
        """Test reading invalid YAML raises StorageError."""
        store.write_text("invalid.yaml", "key: [unclosed")
        with pytest.raises(StorageError):
            store.read_yaml("invalid.yaml")

    def test_read_empty_yaml(self, store):
        """Test reading empty YAML returns empty dict."""
        store.write_text("empty.yaml", "")
        result = store.read_yaml("empty.yaml")
        assert result == {}


class TestPickleOperations:
    """Test pickle read/write operations."""

    def test_write_and_read_pickle(self, store):
        """Test writing and reading pickle."""
        data = {"key": "value", "df": pd.DataFrame({"a": [1, 2, 3]})}
        store.write_pickle("test.pkl", data)
        result = store.read_pickle("test.pkl")
        assert result["key"] == data["key"]
        pd.testing.assert_frame_equal(result["df"], data["df"])


class TestParquetOperations:
    """Test parquet read/write operations."""

    def test_write_and_read_parquet(self, store):
        """Test writing and reading parquet."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        store.write_parquet("test.parquet", df)
        result = store.read_parquet("test.parquet")
        pd.testing.assert_frame_equal(result, df)


class TestFileOperations:
    """Test file operations (exists, delete, copy, list)."""

    def test_exists(self, store):
        """Test exists method."""
        assert not store.exists("test.txt")
        store.write_text("test.txt", "content")
        assert store.exists("test.txt")

    def test_delete_file(self, store):
        """Test deleting a file."""
        store.write_text("test.txt", "content")
        assert store.exists("test.txt")
        store.delete("test.txt")
        assert not store.exists("test.txt")

    def test_delete_directory(self, store):
        """Test deleting a directory."""
        store.write_text("subdir/test.txt", "content")
        assert store.exists("subdir")
        store.delete("subdir")
        assert not store.exists("subdir")

    def test_copy(self, store):
        """Test copying a file."""
        store.write_text("source.txt", "content")
        store.copy("source.txt", "dest.txt")
        assert store.exists("dest.txt")
        assert store.read_text("dest.txt") == "content"

    def test_copy_to_subdir(self, store):
        """Test copying to subdirectory."""
        store.write_text("source.txt", "content")
        store.copy("source.txt", "subdir/dest.txt")
        assert store.exists("subdir/dest.txt")

    def test_list_files(self, store):
        """Test listing files."""
        store.write_text("file1.txt", "a")
        store.write_text("file2.csv", "b")
        store.write_text("subdir/file3.txt", "c")

        all_files = store.list_files()
        assert len(all_files) == 3

        txt_files = store.list_files(suffix=".txt")
        assert len(txt_files) == 2

    def test_list_files_with_prefix(self, store):
        """Test listing files with prefix."""
        store.write_text("data/file1.csv", "a")
        store.write_text("data/file2.csv", "b")
        store.write_text("other/file3.csv", "c")

        data_files = store.list_files(prefix="data")
        assert len(data_files) == 2


class TestExceptionHierarchy:
    """Test exception hierarchy."""

    def test_missing_dependency_is_artifact_store_error(self):
        """Test MissingDependencyError inherits from ArtifactStoreError."""
        assert issubclass(MissingDependencyError, ArtifactStoreError)

    def test_storage_error_is_artifact_store_error(self):
        """Test StorageError inherits from ArtifactStoreError."""
        assert issubclass(StorageError, ArtifactStoreError)
