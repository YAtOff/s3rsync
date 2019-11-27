from io import BytesIO


class DownloadParams(object):
    pass


def download():
    pass


class CursorError(OSError):
    pass


SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2


class Cursor(object):
    @classmethod
    def create(cls, filename):
        with open(filename, "rb") as f:
            data = f.read()
            return cls(data)

    def __init__(self, buffer):
        self.buffer = buffer
        self.size = len(self.buffer)
        self.position = 0

    def seek(self, length, from_position=SEEK_CUR):
        if from_position == SEEK_CUR:
            self.position = self.position + length
        elif from_position == SEEK_SET:
            self.position = length
        elif from_position == SEEK_END:
            self.position = self.size - length

    def read(self, length):
        if length > self.size - self.position:
            raise CursorError("Out of bounds")
        data = self.buffer[self.position:self.position + length]
        self.seek(length)
        return data


class Buffer(object):
    def __init__(self, start, data):
        self.start = start
        self.data = data
        self.length = len(data)
        self.end = self.start + self.length

    def contains(self, start, end):
        return self.start <= start < end <= self.end

    def get(self, start, end):
        if not self.contains(start, end):
            raise CursorError("Out of range")
        return self.data[start - self.start:end - self.start]


class HTTPCursor(object):
    def __init__(self, file):
        self.file = file
        self.position = 0
        self.buffer = Buffer(0, "")

    def seek(self, length, from_position=SEEK_CUR):
        if from_position == SEEK_CUR:
            self.position = self.position + length
        elif from_position == SEEK_SET:
            self.position = length
        elif from_position == SEEK_END:
            self.position = self.size - length

    def read(self, length):
        if not self.buffer.contains(self.position, self.position + length):
            self.buffer_data(length)
        data = self.buffer.get(self.position, self.position + length)
        self.seek(length)
        return data

    def buffer_data(self, length):
        request_data_length = max(1024, length)
        download_params = DownloadParams.from_file_version(
            self.file.latest_version,
            extra_headers={
                "Range": "bytes=%d-%d"
                % (self.position, self.position + request_data_length - 1)
            },
        )
        buffer = BytesIO()
        download(download_params, buffer)
        self.buffer = Buffer(self.position, buffer.getvalue())
