"""ArtifactStore - Unified storage backend for local and S3 operations.

This module provides the ArtifactStore class, which transparently handles
both local filesystem and S3 storage operations.

S3 dependencies (awswrangler, boto3) are optional and only required when
using S3 paths. Install with: pip install artifact-store[cloud]
"""

import glob
import io
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from artifact_store.exceptions import MissingDependencyError, StorageError

logger = logging.getLogger(__name__)

# Lazy import for S3 dependencies
_wr = None
_ClientError = None


def _get_awswrangler():
    """Lazy import awswrangler - only loaded when S3 paths are used."""
    global _wr, _ClientError
    if _wr is None:
        try:
            import awswrangler as wr
            from botocore.exceptions import ClientError

            _wr = wr
            _ClientError = ClientError
        except ImportError as e:
            raise MissingDependencyError(
                "AWS dependencies (awswrangler, boto3) are required for S3 operations. "
                "Install with: pip install artifact-store[cloud]"
            ) from e
    return _wr, _ClientError


class ArtifactStore:
    """Unified storage backend for local filesystem and S3 operations.

    This class provides a single interface for file operations that works
    transparently with both local paths and S3 URIs.

    Parameters
    ----------
    path : str
        Base storage path. Can be:
        - Relative path: resolved to absolute from current working directory
        - Absolute path: used as-is
        - S3 URI: s3://bucket/prefix

    Attributes
    ----------
    base_path : str
        The normalized base path for all operations
    is_s3 : bool
        True if the store is backed by S3, False for local filesystem

    Examples
    --------
    >>> store = ArtifactStore("s3://my-bucket/jobs/123")
    >>> store.write_csv("results/output.csv", df)
    >>> df = store.read_csv("results/output.csv")
    >>>
    >>> # For single file operations with full path:
    >>> store, filename = ArtifactStore.from_file_path("s3://bucket/path/file.csv")
    >>> df = store.read_csv(filename)
    """

    def __init__(self, path: str) -> None:
        """Initialize artifact store with base path.

        Parameters
        ----------
        path : str
            Base storage path (relative, absolute, or S3 URI)
        """
        if path.startswith("s3://"):
            self._base_path = path.rstrip("/")
            self._is_s3 = True
        elif os.path.isabs(path):
            self._base_path = path.rstrip("/\\")
            self._is_s3 = False
        else:
            self._base_path = os.path.abspath(path)
            self._is_s3 = False

    @property
    def base_path(self) -> str:
        """Get the base path for this store.

        Returns
        -------
        str
            The normalized base path
        """
        return self._base_path

    @property
    def is_s3(self) -> bool:
        """Check if this store uses S3 backend.

        Returns
        -------
        bool
            True if S3, False if local filesystem
        """
        return self._is_s3

    @classmethod
    def from_file_path(cls, full_path: str) -> Tuple["ArtifactStore", str]:
        """Create store from a full file path, splitting into base directory and filename.

        Examples
        --------
        >>> store, filename = ArtifactStore.from_file_path("s3://bucket/path/file.csv")
        >>> df = store.read_csv(filename)
        >>>
        >>> store, filename = ArtifactStore.from_file_path("/local/path/file.png")
        >>> store.save_figure(filename, fig)
        """
        if full_path.startswith("s3://"):
            parts = full_path.split("/")
            base_path = "/".join(parts[:-1])
            filename = parts[-1]
        else:
            base_path = os.path.dirname(full_path) or "."
            filename = os.path.basename(full_path)

        return cls(base_path), filename

    def full_path(self, relative_path: str) -> str:
        """Build full path from relative path.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        str
            Full path (S3 URI or local absolute path)

        Examples
        --------
        >>> store = ArtifactStore("/data/project")
        >>> store.full_path("results/output.csv")
        '/data/project/results/output.csv'
        """
        if not relative_path:
            return self._base_path

        relative_path = relative_path.lstrip("/")

        if self._is_s3:
            return f"{self._base_path}/{relative_path}"
        else:
            return os.path.join(self._base_path, relative_path)

    def _ensure_local_dir(self, full_path: str) -> None:
        """Ensure parent directory exists for local paths."""
        if not self._is_s3:
            parent = os.path.dirname(full_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

    # -------------------------------------------------------------------------
    # Core byte operations
    # -------------------------------------------------------------------------

    def read_bytes(self, relative_path: str) -> bytes:
        """Read raw bytes from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        bytes
            File contents as bytes
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            buffer = io.BytesIO()
            wr.s3.download(full_path, buffer)
            buffer.seek(0)
            return buffer.read()
        else:
            with open(full_path, "rb") as f:
                return f.read()

    def write_bytes(self, relative_path: str, data: bytes, content_type: Optional[str] = None) -> None:
        """Write raw bytes to storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        data : bytes
            Data to write
        content_type : str, optional
            Content type hint (used for S3 metadata)
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            buffer = io.BytesIO(data)
            wr.s3.upload(buffer, full_path)
        else:
            self._ensure_local_dir(full_path)
            with open(full_path, "wb") as f:
                f.write(data)

    # -------------------------------------------------------------------------
    # Text operations
    # -------------------------------------------------------------------------

    def read_text(self, relative_path: str, encoding: str = "utf-8") -> str:
        """Read text file from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        encoding : str, optional
            Text encoding, by default "utf-8"

        Returns
        -------
        str
            File contents as string
        """
        data = self.read_bytes(relative_path)
        return data.decode(encoding)

    def write_text(self, relative_path: str, content: str, encoding: str = "utf-8") -> None:
        """Write text content to storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        content : str
            Text content to write
        encoding : str, optional
            Text encoding, by default "utf-8"
        """
        self.write_bytes(relative_path, content.encode(encoding))

    # -------------------------------------------------------------------------
    # CSV operations
    # -------------------------------------------------------------------------

    def read_csv(self, relative_path: str) -> pd.DataFrame:
        """Read CSV file(s) from storage.

        Automatically handles both single files and directories containing
        multiple CSV files (concatenates them).

        Parameters
        ----------
        relative_path : str
            Path relative to base_path (file or directory)

        Returns
        -------
        pd.DataFrame
            DataFrame loaded from CSV file(s)
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            if full_path.lower().endswith(".csv"):
                return wr.s3.read_csv(full_path)
            else:
                return wr.s3.read_csv(full_path, dataset=True)
        else:
            if os.path.isfile(full_path):
                return pd.read_csv(full_path)
            elif os.path.isdir(full_path):
                csv_files = glob.glob(os.path.join(full_path, "**", "*.csv"), recursive=True)
                if not csv_files:
                    raise FileNotFoundError(f"No CSV files found in {full_path}")
                dfs = [pd.read_csv(f, header=0) for f in csv_files]
                return pd.concat(dfs, ignore_index=True)
            else:
                raise FileNotFoundError(f"Path not found: {full_path}")

    def write_csv(self, relative_path: str, df: pd.DataFrame) -> None:
        """Write DataFrame to CSV file.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        df : pd.DataFrame
            DataFrame to write
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            wr.s3.to_csv(df, full_path, index=False)
        else:
            self._ensure_local_dir(full_path)
            df.to_csv(full_path, header=True, index=False)

    # -------------------------------------------------------------------------
    # Parquet operations
    # -------------------------------------------------------------------------

    def read_parquet(self, relative_path: str) -> pd.DataFrame:
        """Read Parquet file(s) from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path (file or directory)

        Returns
        -------
        pd.DataFrame
            DataFrame loaded from Parquet file(s)
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            return wr.s3.read_parquet(full_path)
        else:
            return pd.read_parquet(full_path)

    def write_parquet(self, relative_path: str, df: pd.DataFrame) -> None:
        """Write DataFrame to Parquet file.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        df : pd.DataFrame
            DataFrame to write
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, _ = _get_awswrangler()
            wr.s3.to_parquet(df, full_path, index=False)
        else:
            self._ensure_local_dir(full_path)
            df.to_parquet(full_path, index=False)

    # -------------------------------------------------------------------------
    # Pickle operations
    # -------------------------------------------------------------------------

    def read_pickle(self, relative_path: str) -> Any:
        """Read pickle file from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        Any
            Deserialized Python object
        """
        import pickle

        data = self.read_bytes(relative_path)
        return pickle.loads(data)

    def write_pickle(self, relative_path: str, obj: Any) -> None:
        """Write Python object to pickle file.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        obj : Any
            Python object to serialize
        """
        import pickle

        data = pickle.dumps(obj)
        self.write_bytes(relative_path, data)

    # -------------------------------------------------------------------------
    # JSON operations
    # -------------------------------------------------------------------------

    def read_json(self, relative_path: str) -> Dict[str, Any]:
        """Read JSON file from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        Dict[str, Any]
            Parsed JSON data
        """
        data = self.read_bytes(relative_path)
        return json.loads(data.decode("utf-8"))

    def write_json(self, relative_path: str, data: Dict[str, Any], indent: int = 4) -> None:
        """Write JSON data to storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        data : Dict[str, Any]
            Data to write as JSON
        indent : int, optional
            JSON indentation, by default 4
        """
        json_bytes = json.dumps(data, indent=indent).encode("utf-8")
        self.write_bytes(relative_path, json_bytes)

    # -------------------------------------------------------------------------
    # YAML operations
    # -------------------------------------------------------------------------

    def read_yaml(self, relative_path: str) -> Dict[str, Any]:
        """Read YAML file from storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        Dict[str, Any]
            Parsed YAML data

        Raises
        ------
        StorageError
            If the YAML file contains invalid syntax
        """
        import yaml

        try:
            data = self.read_bytes(relative_path)
            result = yaml.safe_load(data.decode("utf-8"))
            return result if result is not None else {}
        except yaml.YAMLError as e:
            raise StorageError(f"Failed to parse YAML file {relative_path}: {e}") from e

    def write_yaml(self, relative_path: str, data: Dict[str, Any], default_flow_style: bool = False) -> None:
        """Write YAML data to storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        data : Dict[str, Any]
            Data to write as YAML
        default_flow_style : bool, optional
            If True, use flow style (inline) for collections, by default False
        """
        import yaml

        yaml_str = yaml.dump(data, default_flow_style=default_flow_style, allow_unicode=True)
        self.write_bytes(relative_path, yaml_str.encode("utf-8"))

    # -------------------------------------------------------------------------
    # Figure operations
    # -------------------------------------------------------------------------

    def save_figure(self, relative_path: str, fig: Any, **savefig_kwargs: Any) -> None:
        """Save matplotlib figure to storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path
        fig : matplotlib.figure.Figure
            Figure object to save
        **savefig_kwargs
            Additional arguments passed to fig.savefig()
        """
        full_path = self.full_path(relative_path)

        format_map = {".png": "png", ".jpg": "jpeg", ".jpeg": "jpeg", ".pdf": "pdf", ".svg": "svg", ".eps": "eps"}
        file_ext = Path(relative_path).suffix.lower()
        fmt = format_map.get(file_ext, "png")

        if self._is_s3:
            wr, _ = _get_awswrangler()
            buffer = io.BytesIO()
            fig.savefig(buffer, format=fmt, **savefig_kwargs)
            buffer.seek(0)
            wr.s3.upload(buffer, full_path)
        else:
            self._ensure_local_dir(full_path)
            fig.savefig(full_path, format=fmt, **savefig_kwargs)

    # -------------------------------------------------------------------------
    # File operations
    # -------------------------------------------------------------------------

    def exists(self, relative_path: str) -> bool:
        """Check if path exists in storage.

        Parameters
        ----------
        relative_path : str
            Path relative to base_path

        Returns
        -------
        bool
            True if path exists
        """
        full_path = self.full_path(relative_path)

        if self._is_s3:
            wr, ClientError = _get_awswrangler()
            try:
                objects = wr.s3.list_objects(full_path)
                return len(objects) > 0
            except ClientError:
                return False
        else:
            return os.path.exists(full_path)

    def delete(self, relative_path: str = "") -> None:
        """Delete file or directory from storage.

        Parameters
        ----------
        relative_path : str, optional
            Path relative to base_path. If empty, deletes from base_path.
        """
        full_path = self.full_path(relative_path) if relative_path else self._base_path
        logger.info(f"[ArtifactStore] Deleting: {full_path}")

        if self._is_s3:
            wr, _ = _get_awswrangler()
            try:
                wr.s3.delete_objects(full_path)
            except Exception:
                pass
        else:
            if os.path.exists(full_path):
                if os.path.isfile(full_path):
                    os.remove(full_path)
                else:
                    shutil.rmtree(full_path)

    def copy(self, src: str, dest: str) -> None:
        """Copy file within storage or between S3 locations.

        Parameters
        ----------
        src : str
            Source path (relative to base_path, or full S3 URI)
        dest : str
            Destination path (relative to base_path, or full S3 URI)
        """
        src_full = src if src.startswith("s3://") else self.full_path(src)
        dest_full = dest if dest.startswith("s3://") else self.full_path(dest)

        src_is_s3 = src_full.startswith("s3://")
        dest_is_s3 = dest_full.startswith("s3://")

        logger.info(f"[ArtifactStore] Copying: {src_full} -> {dest_full}")

        if src_is_s3 and dest_is_s3:
            wr, _ = _get_awswrangler()
            wr.s3.copy_objects(src_full, dest_full)
        elif not src_is_s3 and not dest_is_s3:
            dest_dir = os.path.dirname(dest_full)
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)
            shutil.copy2(src_full, dest_full)
        # Cross-storage copy would need additional implementation

    def list_files(self, prefix: str = "", suffix: str = "") -> List[str]:
        """List files in storage.

        Parameters
        ----------
        prefix : str, optional
            Path prefix relative to base_path
        suffix : str, optional
            Filter by file suffix (e.g., ".csv")

        Returns
        -------
        List[str]
            List of full paths to matching files
        """
        search_path = self.full_path(prefix) if prefix else self._base_path

        if self._is_s3:
            wr, _ = _get_awswrangler()
            if not search_path.endswith("/"):
                search_path = search_path + "/"

            try:
                if suffix:
                    result = wr.s3.list_objects(search_path, suffix=suffix)
                else:
                    result = wr.s3.list_objects(search_path)
                return [obj for obj in result if obj.startswith(search_path)]
            except Exception:
                return []
        else:
            if suffix:
                pattern = os.path.join(search_path, "**", f"*{suffix}")
            else:
                pattern = os.path.join(search_path, "**", "*")
            return [f for f in glob.glob(pattern, recursive=True) if os.path.isfile(f)]
