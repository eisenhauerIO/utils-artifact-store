"""Artifact Store - Unified storage abstraction for local and S3 operations.

This package provides a single entry point for all file operations,
supporting both local filesystem and S3 storage transparently.

Example
-------
>>> from artifact_store import ArtifactStore
>>>
>>> # Initialize with base path (local or S3)
>>> store = ArtifactStore("/data/project")  # Local
>>> store = ArtifactStore("s3://bucket/prefix")  # S3
>>>
>>> # All operations use relative paths from the base
>>> df = store.read_csv("results/output.csv")
>>> store.write_csv("results/output.csv", df)
>>> store.write_json("config.json", {"key": "value"})
>>> store.write_yaml("settings.yaml", {"debug": True})
>>> store.write_text("logs/run.log", "Started processing...")
>>>
>>> # File operations
>>> store.copy("input/data.csv", "backup/data.csv")
>>> files = store.list_files("results/", suffix=".csv")
>>> store.delete("temp/")
>>>
>>> # Check full resolved path
>>> store.full_path("results/output.csv")  # -> "/data/project/results/output.csv"
"""

__version__ = "0.1.0"

from artifact_store.exceptions import (
    ArtifactStoreError,
    MissingDependencyError,
    StorageError,
)
from artifact_store.jobs import JobInfo, create_job, generate_job_id
from artifact_store.store import ArtifactStore

__all__ = [
    "ArtifactStore",
    "ArtifactStoreError",
    "MissingDependencyError",
    "StorageError",
    "JobInfo",
    "create_job",
    "generate_job_id",
]
