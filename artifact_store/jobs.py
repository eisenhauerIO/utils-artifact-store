"""Job management utilities for artifact storage.

Provides a lightweight JobInfo dataclass for organizing artifacts by job ID,
along with helper functions for job creation.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import pandas as pd

if TYPE_CHECKING:
    from .store import ArtifactStore


@dataclass
class JobInfo:
    """Information about a job and its storage location.

    A job represents a single execution unit (simulation, analysis, etc.)
    with its own isolated storage directory.

    Attributes:
        job_id: Unique identifier for the job
        storage_path: Base storage path where job directories are created
    """

    job_id: str
    storage_path: str

    def __str__(self) -> str:
        return self.job_id

    @property
    def full_path(self) -> str:
        """Get the full path to this job's directory.

        Returns:
            Full path as {storage_path}/{job_id}
        """
        return f"{self.storage_path}/{self.job_id}"

    def get_store(self) -> "ArtifactStore":
        """Get an ArtifactStore for this job's directory.

        Returns:
            ArtifactStore configured for {storage_path}/{job_id}/
        """
        from .store import ArtifactStore

        return ArtifactStore(self.full_path)

    def save_df(self, name: str, df: pd.DataFrame) -> None:
        """Save DataFrame to job directory as CSV.

        Args:
            name: Name of the file (without extension)
            df: DataFrame to save
        """
        self.get_store().write_csv(f"{name}.csv", df)

    def load_df(self, name: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from job directory.

        Args:
            name: Name of the file (without extension)

        Returns:
            DataFrame if file exists, None otherwise
        """
        store = self.get_store()
        path = f"{name}.csv"
        if not store.exists(path):
            return None
        return store.read_csv(path)


def generate_job_id(prefix: str = "job") -> str:
    """Generate a unique job ID with timestamp and short UUID.

    Format: {prefix}-{YYYYMMDD}-{HHMMSS}-{8-char-uuid}

    Args:
        prefix: Prefix for the job ID (default: "job")

    Returns:
        Unique job ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{prefix}-{timestamp}-{short_uuid}"


def _validate_job_id(job_id: str) -> None:
    """Validate job ID for filesystem/S3 safety.

    Args:
        job_id: The job ID to validate

    Raises:
        ValueError: If job_id is empty, too long, or contains invalid characters
    """
    if not job_id:
        raise ValueError("Job ID cannot be empty")

    invalid_chars = set("/\\:\0")
    found_invalid = [c for c in job_id if c in invalid_chars]
    if found_invalid:
        raise ValueError(f"Job ID contains invalid characters: {found_invalid}")

    if len(job_id) > 256:
        raise ValueError("Job ID exceeds maximum length of 256 characters")


def create_job(
    storage_path: str,
    prefix: str = "job",
    job_id: Optional[str] = None,
) -> JobInfo:
    """Create a new JobInfo.

    Args:
        storage_path: Base storage path for job directories
        prefix: Prefix for the job ID (default: "job"), ignored if job_id provided
        job_id: Optional caller-provided job ID. Must contain only
               filesystem-safe characters. If not provided, auto-generated.

    Returns:
        JobInfo with the job_id and storage_path

    Raises:
        ValueError: If job_id contains invalid characters
    """
    if job_id is None:
        job_id = generate_job_id(prefix)
    else:
        _validate_job_id(job_id)

    return JobInfo(job_id=job_id, storage_path=storage_path)
