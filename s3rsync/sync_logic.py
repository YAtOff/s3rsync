from typing import cast

from s3rsync.history import RemoteNodeHistory, NodeHistory
from s3rsync.models import StoredNodeHistory
from s3rsync.node import LocalNode
from s3rsync.sync_action import (
    SyncAction,
    conflict,
    delete_history,
    delete_local,
    delete_remote,
    download,
    nop,
    save_history,
    upload,
)


def handle_node(
    remote: RemoteNodeHistory, local: LocalNode, stored: StoredNodeHistory
) -> SyncAction:
    if not remote and not local and not stored:
        return nop()
    elif not remote and not local and stored:
        return delete_history(stored)
    elif not remote and local and not stored:
        return upload(None, local)
    elif not remote and local and stored:
        return delete_local(local, stored)
    elif remote.exists and not local and not stored:
        return download(remote, None)
    elif remote.exists and not local and stored:
        return delete_remote(remote, stored)
    elif remote.deleted and not local and stored:
        return delete_history(stored)
    elif remote and local and not stored:
        if remote.deleted:
            return delete_local(local, stored)
        elif cast(NodeHistory, remote.history).etag == local.etag:
            return save_history(remote, local)
        else:
            return conflict(remote, local, stored)
    elif remote and local and stored:
        local_updated = local.updated(stored)
        remote_updated = remote.updated(stored)
        if remote.deleted:
            if local_updated:
                return conflict(remote, local, stored)
            else:
                return delete_local(local, stored)
        elif local_updated and remote_updated:
            if cast(NodeHistory, remote.history).etag == local.etag:
                return nop()
            else:
                return conflict(remote, local, stored)
            return nop()
        elif local_updated:
            return upload(remote, local)
        elif remote_updated:
            return download(remote, stored)
        else:
            return nop()
    return nop()
