from flask import Flask, escape, request, stream_with_context, Response

app = Flask(__name__)


@app.route("/")
def hello():
    name = request.args.get("name", "World")
    return f"Hello, {escape(name)}!"


@app.route("/stream")
def streamed_response():
    def generate():
        yield b"Hello "
        yield request.args["name"].encode("utf-8")
        yield b"!"

    return Response(
        stream_with_context(generate()), content_type="application/octet-stream"
    )
