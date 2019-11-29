from __future__ import annotations

import random
import string

import pytest
from faker import Faker, providers

from s3rsync.history import NodeHistory, NodeHistoryEntry
from s3rsync.util.timeutil import now_as_iso

fake = Faker()
fake.add_provider(providers.file)


class Generator:
    @property
    def key(self):
        return NodeHistoryEntry.generate_key()

    @property
    def etag(self):
        return "".join([random.choice("abcdef0123456789") for i in range(32)])

    @property
    def version(self):
        return "".join([random.choice(string.ascii_lowercase) for i in range(32)])

    @property
    def size(self):
        return random.randint(1, 1000)


generate = Generator()


class HistoryBuilder:
    path = fake.file_path().strip("/")

    def new(self) -> HistoryBuilder:
        self.entries: List[NodeHistoryEntry] = []
        self.marks: Dict[str, int] = {}
        return self

    def deleted(self) -> HistoryBuilder:
        self._add_entry(NodeHistoryEntry.create_deleted())
        return self

    def base_only(self, **extra_args) -> HistoryBuilder:
        self._add_entry(
            NodeHistoryEntry.create_base_only(
                generate.key, generate.etag, generate.version, generate.size
            ),
            **extra_args
        )
        return self

    def delta_only(self, **extra_args) -> HistoryBuilder:
        self._add_entry(
            NodeHistoryEntry.create_delta_only(
                generate.key, generate.etag, generate.size
            ),
            **extra_args
        )
        return self

    def whole(self, **extra_args) -> HistoryBuilder:
        delta_size = generate.size
        self._add_entry(
            NodeHistoryEntry(
                key=generate.key,
                deleted=False,
                etag=generate.etag,
                base_version=generate.version,
                base_size=delta_size * 1000000,
                has_delta=True,
                delta_size=delta_size,
                timestamp=now_as_iso()
            ),
            **extra_args
        )
        return self

    def mark(self, name: str) -> HistoryBuilder:
        self.marks[name] = len(self.entries) - 1
        return self

    def slice(self, from_mark: str, to_mark: str) -> List[NodeHistoryEntry]:
        i = self.marks[from_mark]
        j = self.marks[to_mark]
        return self.entries[i + 1:j + 1]

    def build(self) -> NodeHistory:
        return NodeHistory.create(self.path, self.entries)

    def _add_entry(self, entry, **extra_args):
        for k, v in extra_args.items():
            setattr(entry, k, v)
        self.entries.append(entry)


history = HistoryBuilder()


def scenario(*args):
    scenario.counter += 1
    return (scenario.counter, *args)



scenario.counter = 0


@pytest.mark.parametrize(
    "number,stored,remote,expected_diff",
    [
        scenario(
            history.new().base_only().mark("begin").build(),
            history.delta_only().mark("end").build(),
            (history.slice("begin", "end"), False)
        ),
        scenario(
            history.new().base_only().delta_only().mark("begin").build(),
            history.delta_only().mark("end").build(),
            (history.slice("begin", "end"), False)
        ),
        scenario(
            history.new().base_only().delta_only().build(),
            history.mark("begin").whole().mark("end").build(),
            (history.slice("begin", "end"), False)
        ),
        scenario(
            None,
            history.new().mark("begin").base_only().delta_only().mark("end").build(),
            (history.slice("begin", "end"), True)
        ),
        scenario(
            None,
            history.new().base_only().mark("begin").whole().mark("end").build(),
            (history.slice("begin", "end"), True)
        ),
        scenario(
            history.new().base_only().delta_only().build(),
            history.deleted().mark("begin").base_only().mark("end").build(),
            (history.slice("begin", "end"), True)
        ),
        scenario(
            history.new().base_only().build(),
            history
                .delta_only(delta_size=1)
                .mark("begin")
                .whole(base_size=2, delta_size=1)
                .delta_only(delta_size=1)
                .mark("end")
                .build(),
            (history.slice("begin", "end"), True)
        ),
    ]
)
def test_history_diff(number, stored, remote, expected_diff):
    diff, is_absolute = remote.diff(stored)
    assert (diff, is_absolute) == expected_diff
