from pathlib import Path


# Utility function to ensure path is a str
def ensure_path(path: Path | str) -> Path:
    return str(path) if isinstance(path, Path) else path
