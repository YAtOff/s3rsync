import os
from io import IOBase
from queue import Queue
from threading import Thread
from typing import Iterable

from librsync import patch


class BufferReader(IOBase):
    def __init__(self, queue: Queue, name=None):
        self.queue = queue
        self.name = name
        self.offset = 0
        self.buffer = None
        self.buffer_offset = 0

    def read(self, size=-1):
        if size == -1:
            return self.readall()
        elif size < 0:
            raise ValueError
        elif size == 0:
            return b""
        else:
            buffer = bytearray(size)
            bytes_read = self.readinto(buffer)
            print(self.name, "read ->", buffer[:bytes_read])
            return bytes(buffer[:bytes_read])

    def readall(self):
        raise NotImplementedError

    def readinto(self, buffer):
        if self.buffer is None:
            self.buffer = self.queue.get()
        if len(buffer) >= len(self.buffer) - self.buffer_offset:
            buffer[: len(self.buffer) - self.buffer_offset] = self.buffer[self.buffer_offset:]
            result = len(self.buffer) - self.buffer_offset
            self.buffer = None
            self.buffer_offset = 0
        else:
            buffer[:] = self.buffer[self.buffer_offset:self.buffer_offset + len(buffer)]
            self.buffer_offset += len(buffer)
        self.offset += result
        return result

    def seek(self, offset, whence=os.SEEK_SET):
        print(self.name, "seek", self.offset, offset, whence)
        if whence != os.SEEK_SET:
            raise NotImplementedError

        if self.offset == offset:
            return
        elif self.offset < offset:
            self.read(offset - self.offset)
        else:
            raise NotImplementedError


class BufferWriter(IOBase):
    def __init__(self, queue: Queue, name=None):
        self.queue = queue
        self.name = name

    def write(self, buffer):
        print(self.name, "write <-", buffer)
        self.queue.put(buffer)

    def seek(self, offset, whence=os.SEEK_SET):
        print(self.name, "seek", 0, offset, whence)


class ReadWritePair:
    def __init__(self, name=None):
        self.queue = Queue()
        self.name = name
        self.reader = BufferReader(self.queue, name=f"R{self.name}")
        self.writer = BufferWriter(self.queue, name=f"W{self.name}")

    def __iter__(self):
        return iter((self.reader, self.writer))


def pipeline(base: IOBase, deltas: Iterable[IOBase]) -> IOBase:
    input = base
    for i, delta in enumerate(deltas):
        reader, writer = ReadWritePair(name=f"{i}")
        thread = Thread(target=patch, args=(input, delta, writer), daemon=True)
        thread.start()
        input = reader
        result = reader

    return result
