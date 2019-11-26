from dataclasses import dataclass, asdict
from typing import Optional, List, Dict


@dataclass
class FileHistoryEntry:
    base_file: Optional[str]
    delta_file: Optional[str]
    signature_file: str


class FileHistory:
    entries: List[FileHistoryEntry]

    def as_dict(self) -> Dict:
        return {
            "entries": [asdict(e) for e in self.entries]
        }
