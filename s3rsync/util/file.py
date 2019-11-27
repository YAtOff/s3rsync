from pathlib import Path
import hashlib
from typing import Generator


def iter_folder(folder: Path) -> Generator[Path, None, None]:
    for p in folder.iterdir():
        if p.is_file():
            yield p
        elif p.is_dir():
            yield from iter_folder(p)


def hash_path(path: str) -> str:
    return hashlib.new("md5", path.encode("utf-8")).hexdigest()
