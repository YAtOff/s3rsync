import click
import librsync


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.pass_context
@click.argument("base")
@click.argument("signature")
def signature(ctx, base, signature):
    librsync.signature_from_paths(base, signature)


@cli.command()
@click.pass_context
@click.argument("signature")
@click.argument("new")
@click.argument("delta")
def delta(ctx, signature, new, delta):
    librsync.delta_from_paths(signature, new, delta)


@cli.command()
@click.pass_context
@click.argument("base")
@click.argument("delta")
@click.argument("result")
def patch(ctx, base, delta, result):
    librsync.patch_from_paths(base, delta, result)


if __name__ == "__main__":
    cli(obj={})
