import requests


class HTTPRequest:
    def __init__(self, url):
        self.url = url

    def start(self):
        self.response = requests.get(self.url, stream=True)
        self.response.raise_for_status()
        self.content_iter = self.response.iter_content(chunk_size=16 * 4096)
        return self

    def read_chunk(self):
        """ Will raise StopIteration on EOF"""
        return next(self.content_iter)


# TODO: extend io.IOBase
class HTTPSource:
    request = None
    min_buffer_length = 16 * 4096

    def __init__(self, url):
        self.url = url
        self.buffer = None

    def read(self):
        if self.request is None:
            self.request = HTTPRequest(self.url).start()

        # TODO: EOF
        while not self.buffer or len(self.buffer) < self.min_buffer_length:
            self.buffer += self.request.read_chunk()

        result = self.buffer
        self.buffer = None
        return result


class HTTPForwardSeekableSource(HTTPSource):
    pass
