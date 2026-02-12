# Function to find project root
from pathlib import Path

# Function to find project root
def find_project_root(start:Path | None=None) -> Path:
    start = start or Path.cwd()
    for p in [start, *start.parents]:
        # print(p)
        if (p / 'pyproject.toml').exists() or (p / '.git').exists() or (p / 'data').exists():
            return p
    raise FileNotFoundError('Could not find root directory')