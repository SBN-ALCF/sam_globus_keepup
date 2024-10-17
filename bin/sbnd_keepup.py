#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
"""

import sys
import os
import pathlib
import logging
from typing import Optional

from sam_globus_keepup import EXPERIMENT
from sam_globus_keepup.sam import SAMProjectManager
from sam_globus_keepup.globus import GLOBUSSessionManager

from sam_globus_keepup.const import SBND_RAWDATA_REGEXP
from sam_globus_keepup.utils import run_path, check_env


SAM_DATASET = "sbnd_keepup_from_17200_raw_Oct14"

SCRATCH_PATH = pathlib.Path('/scratch/sbnd/rawdata')

# a pure path because the destination file system is not mounted on this machine
EAGLE_PATH = pathlib.PurePosixPath('/neutrinoGPU/sbnd/data')

BUFFER_KB = 100 * 1024 * 1024 


def eagle_run_path(run_number: int) -> pathlib.PurePosixPath:
    """Return 6-digit directory structure for run number ABCDE as 0AB000/0ABC00/0ABCDE."""
    return pathlib.PurePosixPath(*[f'{p * int(run_number / p):06d}' for p in (1000, 100, 1)])


def main():
    check_env("IFDH_PROXY_ENABLE", '0')

    client_id = check_env("GLOBUS_API_CLIENT_ID")
    src_endpoint = check_env("GLOBUS_CEPHFS_COLLECTION_ID")
    dest_endpoint = check_env("GLOBUS_EAGLE_COLLECTION_ID")


    with GLOBUSSessionManager(client_id, src_endpoint, dest_endpoint) as globus_session, \
            SAMProjectManager(project_base=project_base, dataset=SAM_DATASET) as sam_project:

        for f in sam_project.get_files():
            logger.info(f"Starting transfer of {f} to src: {SCRATCH_PATH}")
            result = SBND_RAWDATA_REGEXP.match(f)
            evb, run_number = (int(g) for g in result.groups())
            srcdir = SCRATCH_PATH / run_path(run_number)
            sam_project.save_file(dest=srcdir)

            eagle_dest = EAGLE_PATH / eagle_run_path(run_number)
            globus_session.add_file(outdir / f, eagle_dest / f)

            if du(SCRATCH_PATH) < BUFFER_KB:
                continue

            logger.info(f"Starting transfer of {globus_session.task_nfiles} to dest: {eagle_dest}")
            globus_session.submit()
            globus_session.wait()

            logger.info(f"Transfer finished. Cleaning up files.")
            # TODO remove transferred files, logger.debug


if __name__ == '__main__':
    main()
