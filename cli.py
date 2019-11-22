import click
import librsync


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


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.pass_context
@click.argument("base")
@click.argument("delta")
@click.argument("result")
def patch(ctx, base, delta, result):
    fm = FileManger()
    try:
        librsync.patch(fm.open(base, "rb"), fm.open(delta, "rb"), fm.open(result, "wb"))
    finally:
        fm.close()


if __name__ == "__main__":
    cli(obj={})
