# Artifact Store

Unified storage abstraction for local filesystem and S3 operations.

## Installation

```bash
# Basic installation (local filesystem only)
pip install artifact-store

# With S3 support
pip install artifact-store[cloud]

# From git
pip install git+https://github.com/eisenhauerIO/utils-artifacts-store.git
```

## Quick Start

```python
from artifact_store import ArtifactStore

# Initialize with local path
store = ArtifactStore("/data/project")

# Or with S3 URI
store = ArtifactStore("s3://my-bucket/prefix")

# All operations use relative paths from base
df = store.read_csv("results/output.csv")
store.write_csv("results/output.csv", df)
store.write_json("config.json", {"key": "value"})
store.write_yaml("settings.yaml", {"debug": True})

# File operations
store.copy("input/data.csv", "backup/data.csv")
files = store.list_files("results/", suffix=".csv")
store.delete("temp/")

# Check full resolved path
store.full_path("results/output.csv")  # -> "/data/project/results/output.csv"
```

## Supported Operations

| Method | Description |
|--------|-------------|
| `read_bytes` / `write_bytes` | Raw byte operations |
| `read_text` / `write_text` | Text file operations |
| `read_csv` / `write_csv` | DataFrame CSV operations |
| `read_parquet` / `write_parquet` | DataFrame Parquet operations |
| `read_pickle` / `write_pickle` | Python object serialization |
| `read_json` / `write_json` | JSON operations |
| `read_yaml` / `write_yaml` | YAML operations |
| `save_figure` | Matplotlib figure saving |
| `exists` | Check if path exists |
| `delete` | Delete file or directory |
| `copy` | Copy files |
| `list_files` | List files with optional prefix/suffix filter |

## S3 Support

S3 operations require the `[cloud]` extra which includes `awswrangler` and `boto3`. These dependencies are lazy-loaded - they're only imported when you use an S3 path.

```python
# This works without cloud dependencies
store = ArtifactStore("/local/path")
store.read_csv("data.csv")

# This requires cloud dependencies
store = ArtifactStore("s3://bucket/prefix")  # Raises MissingDependencyError if not installed
```

## License

MIT
