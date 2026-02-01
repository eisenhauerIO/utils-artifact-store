"""Tests for jobs module."""

import re
import shutil
import tempfile

import pandas as pd
import pytest

from artifact_store import JobInfo, create_job, generate_job_id


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


class TestGenerateJobId:
    """Test generate_job_id function."""

    def test_default_prefix(self):
        """Test job ID with default prefix."""
        job_id = generate_job_id()
        assert job_id.startswith("job-")

    def test_custom_prefix(self):
        """Test job ID with custom prefix."""
        job_id = generate_job_id(prefix="sim")
        assert job_id.startswith("sim-")

    def test_format(self):
        """Test job ID format matches expected pattern."""
        job_id = generate_job_id()
        # Format: {prefix}-{YYYYMMDD}-{HHMMSS}-{8-char-uuid}
        pattern = r"^job-\d{8}-\d{6}-[a-f0-9]{8}$"
        assert re.match(pattern, job_id), f"Job ID {job_id} doesn't match expected format"

    def test_uniqueness(self):
        """Test that generated IDs are unique."""
        ids = {generate_job_id() for _ in range(100)}
        assert len(ids) == 100


class TestCreateJob:
    """Test create_job function."""

    def test_auto_generated_id(self, temp_dir):
        """Test create_job with auto-generated ID."""
        job = create_job(temp_dir)
        assert job.job_id.startswith("job-")
        assert job.storage_path == temp_dir

    def test_custom_prefix(self, temp_dir):
        """Test create_job with custom prefix."""
        job = create_job(temp_dir, prefix="sim")
        assert job.job_id.startswith("sim-")

    def test_caller_provided_id(self, temp_dir):
        """Test create_job with caller-provided ID."""
        job = create_job(temp_dir, job_id="my-custom-job-123")
        assert job.job_id == "my-custom-job-123"
        assert job.storage_path == temp_dir

    def test_caller_id_ignores_prefix(self, temp_dir):
        """Test that prefix is ignored when job_id is provided."""
        job = create_job(temp_dir, prefix="ignored", job_id="my-job")
        assert job.job_id == "my-job"


class TestJobIdValidation:
    """Test job ID validation."""

    def test_empty_id_rejected(self, temp_dir):
        """Test that empty job ID is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            create_job(temp_dir, job_id="")

    def test_forward_slash_rejected(self, temp_dir):
        """Test that forward slash in job ID is rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            create_job(temp_dir, job_id="job/with/slashes")

    def test_backslash_rejected(self, temp_dir):
        """Test that backslash in job ID is rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            create_job(temp_dir, job_id="job\\with\\backslash")

    def test_colon_rejected(self, temp_dir):
        """Test that colon in job ID is rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            create_job(temp_dir, job_id="job:with:colon")

    def test_null_byte_rejected(self, temp_dir):
        """Test that null byte in job ID is rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            create_job(temp_dir, job_id="job\x00null")

    def test_too_long_id_rejected(self, temp_dir):
        """Test that too-long job ID is rejected."""
        long_id = "a" * 257
        with pytest.raises(ValueError, match="exceeds maximum length"):
            create_job(temp_dir, job_id=long_id)

    def test_max_length_id_accepted(self, temp_dir):
        """Test that max-length job ID is accepted."""
        max_id = "a" * 256
        job = create_job(temp_dir, job_id=max_id)
        assert job.job_id == max_id


class TestJobInfo:
    """Test JobInfo dataclass."""

    def test_str(self, temp_dir):
        """Test string representation."""
        job = JobInfo(job_id="test-job", storage_path=temp_dir)
        assert str(job) == "test-job"

    def test_full_path(self, temp_dir):
        """Test full_path property."""
        job = JobInfo(job_id="test-job", storage_path=temp_dir)
        assert job.full_path == f"{temp_dir}/test-job"

    def test_get_store(self, temp_dir):
        """Test get_store returns ArtifactStore."""
        job = JobInfo(job_id="test-job", storage_path=temp_dir)
        store = job.get_store()
        assert store.base_path == f"{temp_dir}/test-job"

    def test_save_and_load_df(self, temp_dir):
        """Test save_df and load_df methods."""
        job = JobInfo(job_id="test-job", storage_path=temp_dir)
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        job.save_df("results", df)
        loaded = job.load_df("results")

        pd.testing.assert_frame_equal(loaded, df)

    def test_load_df_nonexistent(self, temp_dir):
        """Test load_df returns None for nonexistent file."""
        job = JobInfo(job_id="test-job", storage_path=temp_dir)
        result = job.load_df("nonexistent")
        assert result is None
