"""Job management utilities for artifact storage.

Provides a lightweight JobInfo dataclass for organizing artifacts by job ID,
along with helper functions for job creation and ID generation.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

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

    def get_store(self) -> "ArtifactStore":
        """Get an ArtifactStore for this job's directory.

        Returns:
            ArtifactStore configured for {storage_path}/{job_id}/
        """
        from .store import ArtifactStore

        return ArtifactStore(f"{self.storage_path}/{self.job_id}")


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
