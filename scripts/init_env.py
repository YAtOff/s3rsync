from textwrap import dedent

import click

from dynaconf import settings


@click.command()
@click.argument("s3_prefix")
def init_env(s3_prefix):
    """
    Run:
    source <(python scripts/init_env.py s3_prefix)

    """
    print("alias s3ls='aws s3 ls'")
    print("alias s3rm='aws s3 rm'")
    print("alias s3cp='aws s3 cp'")
    print(f'export rs_prefix="{s3_prefix}"')
    print(f'export rs_storage_bucket="{settings.STORAGE_BUCKET}"')
    print(f'export rs_internal_bucket="{settings.INTERNAL_BUCKET}"')
    print(f'export rs_metadata_prefix="{settings.SYNC_METADATA_PREFIX}"')
    print(f'export rs_history="s3://$rs_internal_bucket/$rs_prefix/$rs_metadata_prefix/history"')
    print(f'export rs_entries="s3://$rs_internal_bucket/$rs_prefix/$rs_metadata_prefix/entries"')
    print(f'export rs_storage="s3://$rs_storage_bucket/$rs_prefix"')
    print(dedent("""
        rs_dump_history() {
            f=$(mktemp) && \\
            aws s3 cp "$rs_history/$1" $f > /dev/null && \\
            (cat $f | jq '.') && \\
            rm -rf $f
        }

        export dump_history
    """))


if __name__ == "__main__":
    init_env()
