import sys

from s3rsync.stream import pipeline


class FileManger:
    def __init__(self):
        self.fds = []

    def open(self, *args, **kwargs):
        fd = open(*args, **kwargs)
        self.fds.append(fd)
        return fd

    def close(self):
        for fd in self.fds:
            fd.close()


fm = FileManger()
try:
    reader = pipeline(
        fm.open("./data/file1", "rb"), [fm.open("./data/delta2", "rb"), fm.open("./data/delta3", "rb")]
    )
    while True:
        buf = reader.read(1024)
        if not buf:
            break
        print("result:", buf)
        sys.exit(0)
finally:
    fm.close()
