#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
"""

import time
import sys
import os
import pathlib
import functools
from typing import Optional

from sam_globus_keepup import EXPERIMENT, IFDH_Client
from sam_globus_keepup.sam import SAMProjectManager
from sam_globus_keepup.globus import GLOBUSSessionManager
from sam_globus_keepup.mon import NetworkMonitor

from sam_globus_keepup.const import SBND_RAWDATA_REGEXP
from sam_globus_keepup.utils import run_path, check_env, du

import logging
logger = logging.getLogger(__name__)

SAM_PROJECT_BASE = 'globus_dtn_xfer_test3'
SAM_DATASET = "sbnd_keepup_from_17400_raw_Oct25"

SCRATCH_PATH = pathlib.Path('/ceph/sbnd/rawdata')

# a pure path because the destination file system is not mounted on this machine
EAGLE_PATH = pathlib.PurePosixPath('/neutrinoGPU/sbnd/data')

BUFFER_KB = 100 * 1024 * 1024 
GLOBUS_NFILE_MAX = 10000


def eagle_run_path(run_number: int) -> pathlib.PurePosixPath:
    """Return 6-digit directory structure for run number ABCDE as 0AB000/0ABC00/0ABCDE."""
    return pathlib.PurePosixPath(*[f'{p * int(run_number / p):06d}' for p in (1000, 100, 1)])


def ifdh_cp_run_number(fname: str, dest_base: Optional[pathlib.Path]=None, dest_is_dir: bool=True):
    """Variation of ifdh_cp function but we use the file name to set the output path."""
    dest = dest_base
    if dest_base is None:
        # use current directory
        dest = pathlib.Path().resolve()
        dest_is_dir = True

    if dest_is_dir:
        # directory based on run number from file name
        result = SBND_RAWDATA_REGEXP.match(fname)
        evb, run_number = (int(g) for g in result.groups())
        dest = dest_base / run_path(run_number)
        dest.mkdir(parents=True, exist_ok=True)

    IFDH_Client.cp([fname, str(dest)])


def scratch_eagle_paths(filename: str):
    """Return corresponding paths on both scratch and eagle for filename."""
    result = SBND_RAWDATA_REGEXP.match(str(filename))
    evb, run_number = (int(g) for g in result.groups())
    srcdir = SCRATCH_PATH / run_path(run_number)

    f_basename = pathlib.PurePath(filename).name

    eagle_dest = EAGLE_PATH / eagle_run_path(run_number)
    return srcdir / f_basename, eagle_dest / f_basename


def main():
    check_env("IFDH_PROXY_ENABLE", '0')

    client_id = check_env("GLOBUS_API_CLIENT_ID")
    src_endpoint = check_env("GLOBUS_CEPHFS_COLLECTION_ID")
    dest_endpoint = check_env("GLOBUS_EAGLE_COLLECTION_ID")

    nm1 = NetworkMonitor('eno1', 60, 'ip_log_eno1.txt')
    nm2 = NetworkMonitor('eno2', 60, 'ip_log_eno2.txt')
    nm1.start()
    nm2.start()

    sam_callback = functools.partial(ifdh_cp_run_number, dest_base=SCRATCH_PATH)
    with GLOBUSSessionManager(client_id, src_endpoint, dest_endpoint) as globus_session, \
           SAMProjectManager(project_base=SAM_PROJECT_BASE, dataset=SAM_DATASET, parallel=8) as sam_project:

        # check if there are outstanding files. If so, try to transfer these
        logger.debug(f"Checking for outstanding files at {SCRATCH_PATH}...")
        for f in SCRATCH_PATH.glob('**/*.root'):
            src, dest = scratch_eagle_paths(f)
            globus_session.add_file(src, dest)

        if globus_session.task_nfiles > GLOBUS_NFILE_MAX:
            logger.info(f"Starting transfer of {globus_session.task_nfiles} to dest: {EAGLE_PATH}")
            globus_session.submit()
        else:
            logger.info(f"Starting with nfiles={globus_session.task_nfiles}")

        # start file transfer with SAM + ifdh
        sam_project.start(callback=sam_callback)
        count = 0
        while True:
            f = sam_project.get_file(timeout=1)
            if f is None:
                logger.debug(f"No queued files. GLOBUS sleeping")
                time.sleep(10)
                continue
            
            count += 1

            src, dest = scratch_eagle_paths(f)
            globus_session.add_file(src, dest)

            # don't start a new transfer until we have BUFFER_KB of data
            # if du(SCRATCH_PATH) < BUFFER_KB or globus_session.running():
            if count < GLOBUS_NFILE_MAX:
                logger.debug(f"GLOBUS: Waiting for {GLOBUS_NFILE_MAX} files ({count=})")
                continue

            # don't start if we have an outstanding task
            running_task = globus_session.running()
            if running_task:
                logger.debug(f"GLOBUS: Waiting for task to complete ({running_task=})")
                continue

            count = 0
            logger.info(f"Starting transfer of {globus_session.task_nfiles} to dest: {eagle_dest}")
            globus_session.submit()

    nm1.stop()
    nm2.stop()


if __name__ == '__main__':
    logging.basicConfig(filename='globus_xfer.log',
            format='[%(asctime)s] (%(levelname)s) %(name)s: %(message)s',
            filemode='a',level=logging.DEBUG)
    main()
