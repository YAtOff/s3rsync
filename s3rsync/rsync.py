from dataclasses import dataclass
from pathlib import Path

from s3rsync.history import FileHistory


@dataclass
class FileOptions:
    main_bucket: str
    main_bucket_prefix: str
    rsync_bucket: str
    rsync_bucket_prefix: str


@dataclass
class File:
    root: Path
    path: str
    optins: FileOptions

    @property
    def history(self) -> FileHistory:
        pass

    @property
    def s3_path(self) -> str:
        return Path(self.path).relative_to(self.root).as_posix()
