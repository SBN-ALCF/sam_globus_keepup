#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
Modified from sbnd_keepup.py to ignore run number, and use a different data set
"""

import time
import sys
import os
import pathlib
import functools
import hashlib
from typing import Optional

from sam_globus_keepup import EXPERIMENT, IFDH_Client
from sam_globus_keepup.sam import SAMProjectManager
from sam_globus_keepup.globus import GLOBUSSessionManager
from sam_globus_keepup.mon import NetworkMonitor

from sam_globus_keepup.const import SBND_RAWDATA_REGEXP
from sam_globus_keepup.utils import run_path, check_env, du

import logging
logger = logging.getLogger(__name__)

SAM_PROJECT_BASE = 'globus_dtn_xfer_test7'
SAM_DATASET = "gputnam-Run4-bnbmajority-prestage-A"

SCRATCH_PATH = pathlib.Path('/srv/globus/icarus/run4')

EAGLE_PATH = pathlib.PurePosixPath('/icarus/run4')

BUFFER_KB = 100 * 1024 * 1024 
GLOBUS_NFILE_MAX = 50


def hash_path(filename: pathlib.Path):
    """Return a path based on the first two digits of the filename's hash.
    Note that this function might be given a protocol path as str (XXXX://...)
    or a pathlib path. Try to use pathlib, otherwise just split."""

    basename = filename
    try:
        basename = filename.name
    except AttributeError:
        basename = filename.split("/")[-1]

    return hashlib.sha256(basename.encode('utf-8')).hexdigest()[:2]


def ifdh_cp_run_number(fname: str, dest_base: Optional[pathlib.Path]=None, dest_is_dir: bool=True):
    """Variation of ifdh_cp function but we use the file name to set the output path."""
    dest = dest_base
    if dest_base is None:
        # use current directory
        dest = pathlib.Path().resolve()
        dest_is_dir = True

    if dest_is_dir:
        # directory based on run number from file name
        dest = dest_base / hash_path(fname)
        dest.mkdir(parents=True, exist_ok=True)

    IFDH_Client.cp([fname, str(dest)])


def scratch_eagle_paths(filename: str):
    """Return corresponding paths on both scratch and eagle for filename."""
    srcdir = SCRATCH_PATH / hash_path(filename)

    f_basename = pathlib.PurePath(filename).name

    eagle_dest = EAGLE_PATH / hash_path(filename)
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

    main_loop(client_id, src_endpoint, dest_endpoint)

    # accumulate a few data points from the network monitors after all transfers conclude
    time.sleep(600)

    nm1.stop()
    nm2.stop()


def main_loop(client_id, src_endpoint, dest_endpoint):
    # check if there are outstanding files. If so, try to transfer these before starting SAM
    with GLOBUSSessionManager(client_id, src_endpoint, dest_endpoint) as globus_session:
        logger.debug(f"Checking for outstanding files at {SCRATCH_PATH}...")
        nfiles_outstanding = 0 
        for f in SCRATCH_PATH.glob('**/*.root'):
            src, dest = scratch_eagle_paths(f)
            globus_session.add_file(src, dest)
            nfiles_outstanding += 1
            if nfiles_outstanding >= GLOBUS_NFILE_MAX:
                logger.info(f"Starting transfer of {globus_session.task_nfiles} to dest: {EAGLE_PATH}")
                globus_session.submit()
                time.sleep(10)
                globus_session.wait()
                nfiles_outstanding = 0

        # start SAM project once all outstanding files have been transferred

        with SAMProjectManager(project_base=SAM_PROJECT_BASE, dataset=SAM_DATASET, parallel=8) as sam_project:
            if sam_project.nfiles == 0:
                logger.info(f"SAM project has no files, exiting.")
                return

            # start file transfer with SAM + ifdh
            # wait a few seconds for files to appear in the queue
            sam_callback = functools.partial(ifdh_cp_run_number, dest_base=SCRATCH_PATH)
            sam_project.start(callback=sam_callback, check_locality=True)
            time.sleep(10)

            nfiles = 0
            nsleep = 0
            while True:
                logger.debug(f"Checking for new files")
                f = sam_project.get_file()
                if f is None:
                    # break after no new files
                    nsleep += 1

                    # sometimes SAM project gets a stale process
                    if (nsleep > 10 and not sam_project.running()) or nsleep > 1000:
                        logger.debug(f"Transferring outstanding files and exiting!")
                        globus_session.submit()
                        # prevents wait call from happening before submission is finished 
                        time.sleep(10)
                        globus_session.wait()
                        break

                    logger.debug(f"No queued files. GLOBUS sleeping {nsleep=}")

                    time.sleep(10)
                    continue
                
                nfiles += 1
                nsleep = 0

                src, dest = scratch_eagle_paths(f)
                globus_session.add_file(src, dest)

                # don't start a new transfer until we have BUFFER_KB of data
                # if du(SCRATCH_PATH) < BUFFER_KB or globus_session.running():
                if nfiles < GLOBUS_NFILE_MAX:
                    logger.debug(f"GLOBUS: Waiting for {GLOBUS_NFILE_MAX} files ({nfiles=})")
                    continue

                # don't start if we have an outstanding task
                running_task = globus_session.running()
                if running_task:
                    logger.debug(f"GLOBUS: Waiting for task to complete ({running_task=})")
                    continue

                nfiles = 0
                nsleep = 0
                logger.info(f"Starting transfer of {globus_session.task_nfiles} to dest: {dest_endpoint}")
                globus_session.submit()



if __name__ == '__main__':
    logging_fmt = '[%(asctime)s] (%(levelname)s) %(name)s: %(message)s'
    logging.basicConfig(filename='sam_xfer.log',
            format=logging_fmt, filemode='a',level=logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(logging_fmt)
    stdout_handler.setFormatter(formatter)
    root = logging.getLogger()
    root.addHandler(stdout_handler)
    main()
