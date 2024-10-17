#!/usr/bin/env python

import os
import sys
import pathlib
import logging

from sam_globus_keepup.globus import GLOBUSSessionManager


def main():
    client_id = os.environ['GLOBUS_API_CLIENT_ID']
    src_endpoint = os.environ['GLOBUS_CEPHFS_COLLECTION_ID']
    dest_endpoint = os.environ['GLOBUS_EAGLE_COLLECTION_ID']
    with GLOBUSSessionManager(client_id, src_endpoint, dest_endpoint) as session:
        session.add_file("/scratch/sbnd/file1.txt", "/neutrinoGPU/file1.txt")
        session.submit()
        session.wait()


if __name__ == "__main__":
    if "GLOBUS_API_CLIENT_ID" not in os.environ:
        raise RuntimeError("Must set GLOBUS_API_CLIENT_ID environment variable.")

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()
