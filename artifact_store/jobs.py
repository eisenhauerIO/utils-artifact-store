"""Job management utilities for artifact storage.

Provides a lightweight JobInfo dataclass for organizing artifacts by job ID,
along with helper functions for job creation, listing, and cleanup.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from .store import ArtifactStore


@dataclass
class JobInfo:
    """Information about a job and its storage location.

    A job represents a single execution unit (simulation, analysis, etc.)
    with its own isolated storage directory.

    Attributes:
        job_id: Unique identifier for the job (e.g., "job-20251221-123456-abc12345")
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


def create_job(storage_path: str, prefix: str = "job") -> JobInfo:
    """Create a new JobInfo with auto-generated ID.

    Args:
        storage_path: Base storage path for job directories
        prefix: Prefix for the job ID (default: "job")

    Returns:
        JobInfo with generated job_id and the specified storage_path
    """
    return JobInfo(job_id=generate_job_id(prefix), storage_path=storage_path)


def list_jobs(storage_path: str, prefix: str = "job") -> List[str]:
    """List all job IDs in a storage path.

    Args:
        storage_path: Base path where job directories are stored
        prefix: Job ID prefix to filter by (default: "job")

    Returns:
        List of job IDs sorted by creation time (newest first)
    """
    output_dir = Path(storage_path)
    if not output_dir.exists():
        return []

    jobs = [
        d.name for d in output_dir.iterdir()
        if d.is_dir() and d.name.startswith(f"{prefix}-")
    ]
    return sorted(jobs, reverse=True)


def cleanup_old_jobs(storage_path: str, prefix: str = "job", keep: int = 10) -> List[str]:
    """Clean up old job directories, keeping only the most recent ones.

    Args:
        storage_path: Base path where job directories are stored
        prefix: Job ID prefix to filter by (default: "job")
        keep: Number of recent jobs to keep (default: 10)

    Returns:
        List of removed job IDs
    """
    jobs = list_jobs(storage_path, prefix)
    if len(jobs) <= keep:
        return []

    removed = []
    for job_id in jobs[keep:]:
        job = JobInfo(job_id=job_id, storage_path=storage_path)
        store = job.get_store()
        if store.exists(""):
            store.delete()
            removed.append(job_id)
    return removed
