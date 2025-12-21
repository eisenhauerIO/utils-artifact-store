"""Custom exceptions for the artifact_store package.

This module defines exception classes for handling errors in storage operations.
"""


class ArtifactStoreError(Exception):
    """Base exception for artifact store errors.

    All exceptions raised by the artifact_store package inherit from this class,
    allowing users to catch all artifact store errors with a single except clause.

    Examples
    --------
    >>> try:
    ...     store.read_yaml("config.yaml")
    ... except ArtifactStoreError as e:
    ...     print(f"Storage error: {e}")
    """

    pass


class MissingDependencyError(ArtifactStoreError):
    """Raised when required optional dependencies are not installed.

    This exception is raised when attempting S3 operations without having
    the required AWS dependencies (awswrangler, boto3) installed.

    Examples
    --------
    >>> store = ArtifactStore("s3://bucket/prefix")
    >>> store.read_csv("data.csv")  # Raises if awswrangler not installed
    MissingDependencyError: AWS dependencies (awswrangler, boto3) are required...
    """

    pass


class StorageError(ArtifactStoreError):
    """Raised when a storage operation fails.

    This exception wraps underlying storage errors (file not found, permission
    denied, parse errors, etc.) with additional context about the operation.

    Examples
    --------
    >>> store.read_yaml("invalid.yaml")  # File contains invalid YAML
    StorageError: Failed to parse YAML file invalid.yaml: ...
    """

    pass
