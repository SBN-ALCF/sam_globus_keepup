#!/usr/bin/env python

import os
import sys
import pathlib
import logging

from sam_globus_keepup.globus import GLOBUSSessionManager
from sam_globus_keepup.utils import check_env


def main():
    client_id = check_env("GLOBUS_API_CLIENT_ID")
    client_secret = check_env("GLOBUS_APP_SECRET")
    src_endpoint = check_env("GLOBUS_CEPHFS_COLLECTION_ID")
    dest_endpoint = check_env("GLOBUS_EAGLE_COLLECTION_ID")

    with GLOBUSSessionManager(client_id, src_endpoint, dest_endpoint) as session:
        session.add_file("/ceph/sbnd/file1.txt", "/file1.txt")
        session.submit()
        session.wait()


if __name__ == "__main__":
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()
