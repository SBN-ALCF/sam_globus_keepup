#!/usr/bin/env python3
"""
This program is meant to be a faster replacement of Fermilab's File Transfer
Service (FTS).  In FTS, file transfer and declaration to SAM are serialized,
resulting in a big inefficiency.  Here, we loop over files in a directory and
queue them for declaration. Separately, we queue declared files for transfer.
Transfer and declaration occur in separate processes.
"""

import sys
import time
import argparse
import os
import json
import pathlib
import multiprocessing
import queue
import tempfile
import random
import logging
from datetime import datetime
from typing import Optional

import samweb_client
import samweb_client.utility

import ifdh


SAMWeb_Client = samweb_client.SAMWebClient()
IFDH_Client = ifdh.ifdh()

TRANSFER_NPROCESS_MAX = 10 # max processes
DECLARE_NPROCESS_MAX = 4 # max processes

SAM_RPS_MAX = 5.0 # max requests per second per process
NFILES_MIN = 10 # only spawn a new process after seeing at least this many files
SMEAR_MAX = 1.1 # random factor to smear out the time between requests. Set to 1 for no smearing

HEARTBEAT = '__heartbeat__'

logger = logging.getLogger(__name__)


class MetadataNotFoundException(Exception):
    pass


try:
    EXPERIMENT = os.environ["EXPERIMENT"]
except KeyError:
    raise RuntimeError("Must set EXPERIMENT environment variable.")

try:
    os.environ["IFDH_CP_MAXRETRIES"]
except KeyError:
    raise RuntimeError("Must set IFDH_CP_MAXRETRIES environment variable.")


def require_file(filename:pathlib.Path) -> None:
    """Raise if the file is missing or the path is a directory."""
    if filename.is_dir():
        raise IsADirectoryError(f'{filename} is a directory.')

    # if not filename.is_file():
    #     raise FileNotFoundError(f'Error accessing {filename}. Does it exist?')


def file_size(filename: pathlib.Path) -> int:
    """File size in bytes."""
    require_file(filename)
    return filename.stat().st_size


def dest_path(fname: pathlib.Path, dest_base: Optional[pathlib.PurePosixPath]=None, relative_to: Optional[pathlib.Path]=None):
    """
    Convert a local path to a destination path.
    Example: fname: /local/scratch/reco/myfile.root
             dest_base: /pnfs/users/test
             relative_to: /local/scratch
             -> /pnfs/users/test/reco/myfile.root
    """
    dest = dest_base
    if dest_base is None:
        # use current directory
        dest = pathlib.Path().resolve()

    dir_append = fname.parent
    if relative_to is not None:
        dir_append = fname.parent.relative_to(relative_to)

    return dest / dir_append / get_filename(fname).name


def metadata_file(filename: pathlib.Path) -> pathlib.Path:
    """Expected metadata filename, appends .json."""
    require_file(filename)
    stem = filename.suffix
    return filename.with_suffix('.'.join([stem, 'json']))


def get_filename(filename: pathlib.Path) -> pathlib.Path:
    """Wrapper to get the file's ultimate name, even if different from the original."""
    newname = filename.name.replace('reco1', 'stage0')
    newname = newname.replace('reco2', 'stage1')
    newname = newname.replace('Supplemental', 'hist')
    return filename.parent / newname


def get_metadata(filename: pathlib.Path, do_file_size=True, do_checksum=True) -> dict:
    """
    Return metadata dict including common additions: file size & checksums.
    This function is sprawling because it handles all the different cases, probably better to refactor
    """
    result = {}
    # SBND
    # common
    result['group'] = 'sbnd'
    result["sbnd_project.software"]: "sbndcode"
    VERSION = 'v10_06_00_10'

    # handle cases based on filename
    fname = filename.stem
    if fname.startswith('hist'):
        raise RuntimeError(f"Metadata generation for file with name {fname} is not supported.")
    else:
        # expect other files to have an associated .json file
        meta_filename = metadata_file(filename)

        if not meta_filename.is_file():
            raise MetadataNotFoundException(f'Tried to declare {filename} but {meta_filename} was not found!.')

        with open(meta_filename, 'r') as f:
            result = json.load(f)

        result['file_format'] = 'artroot'
        result['file_type'] = 'data'
        result['data_tier'] = 'reconstructed'
        result['production.type'] = 'aurora'
        result["icarus_project.name"]: "icarus_2025_wcdnn_Run4_offbeam_v10_06_00_01p05"
        result["icarus_project.version"]: VERSION
        result["production.name"]: "offline"

        if fname.startswith('reco1'):
            raise RuntimeError(f"Metadata generation for file with name {fname} is not supported.")
        elif fname.startswith('reco2'):
            result['application'] = { 'family': 'art', 'name': 'stage1_caf_larcv', 'version': VERSION }
            result['fcl.name'] = "stage0_run2_wcdnn_icarus.fcl"
            result["icarus_project.stage"] = "stage1"
            del result['parents']

    """
    # ICARUS
    # common
    result['group'] = 'icarus'
    result["icarus_project.software"]: "icaruscode"
    VERSION = 'v10_06_00_01p05'

    # handle cases based on filename
    fname = filename.stem
    if fname.startswith('Supplemental'):
        # no json file; we'll do our best to get metadata from the file name
        # this extracts the raw data filename from the hist file name
        parent_name = '_'.join(filename.name.split('Supplemental-')[1].split('-stage1.root')[0].split('_')[:-1]) + '.root'

        result['file_type'] = 'data'
        result['fcl.name'] = "stage0_run2_wcdnn_icarus.fcl"
        result['production.type'] = 'polaris'
        result["icarus_project.name"]: "icarus_2025_wcdnn_Run4_offbeam_v10_06_00_01p05"
        result["icarus_project.version"]: VERSION
        if fname.endswith('stage0'):
            result['file_format'] = 'purity'
            result['application'] = { 'family': 'art', 'name': 'stage0', 'version': VERSION }
            result['parents'] = [{'file_name': parent_name}]
        elif fname.endswith('stage1'):
            result['file_format'] = 'calib_ntuples'
            result['application'] = { 'family': 'art', 'name': 'stage1_caf_larcv', 'version': VERSION }
            result['parents'] = [{'file_name': 'stage1-000-' + get_filename(filename.parent / parent_name).name}]
        else:
            raise RuntimeError(f"Metadata generation for file with name {fname} is not supported.")
    else:
        # expect other files to have an associated .json file
        meta_filename = metadata_file(filename)

        if not meta_filename.is_file():
            raise MetadataNotFoundException(f'Tried to declare {filename} but {meta_filename} was not found!.')

        with open(meta_filename, 'r') as f:
            result = json.load(f)

        result['file_format'] = 'artroot'
        result['file_type'] = 'data'
        result['data_tier'] = 'reconstructed'
        result['production.type'] = 'polaris'
        result["icarus_project.name"]: "icarus_2025_wcdnn_Run4_offbeam_v10_06_00_01p05"
        result["icarus_project.version"]: VERSION
        result["production.name"]: "offline"

        if fname.startswith('reco1'):
            result['application'] = { 'family': 'art', 'name': 'stage0', 'version': VERSION }
            result['fcl.name'] = "stage0_run2_wcdnn_icarus.fcl"
            result["icarus_project.stage"] = "stage0"
            result["data_stream"] = "bnbmajority"
            result["art.process_name"] = "stage0"
        elif fname.startswith('reco2'):
            result['application'] = { 'family': 'art', 'name': 'stage1_caf_larcv', 'version': VERSION }
            result['fcl.name'] = "stage0_run2_wcdnn_icarus.fcl"
            result["icarus_project.stage"] = "stage1"
            result['parents'][0]['file_name'] = result['parents'][0]['file_name'].replace('reco1', 'stage0')
    """

    # file name replacements
    result['file_name'] = get_filename(filename).name

    if do_file_size:
        result['file_size'] = file_size(filename)
    else:
        result['file_size'] = 0

    if do_checksum:
        result['checksum'] = samweb_client.utility.fileChecksum(\
                str(filename), checksum_types=['enstore', 'adler32', 'md5'])

    return result


def is_virtual_file(fname: pathlib.Path) -> bool:
    return fname.name.startswith('stage1') or fname.name.startswith('reco2')


def declare_file(filename: pathlib.Path, dest: pathlib.Path, validate=False, delete=False):
    """Declare a file to SAM & add its file location. Several exceptions to not copy back reco2/stage1 files"""
    dest_filename = get_filename(filename)

    is_virtual = is_virtual_file(dest_filename)
    do_file_ops = not is_virtual
    d = get_metadata(filename, do_file_size=do_file_ops, do_checksum=do_file_ops)

    if validate:
        SAMWeb_Client.validateFileMetadata(d)

    SAMWeb_Client.declareFile(d)
    if not is_virtual:
        SAMWeb_Client.addFileLocation(dest_filename.name, dest)
    else:
        logger.info(f'NOT adding file location for virtual file {filename}')

    if delete:
        meta_filename = metadata_file(filename)
        meta_filename.unlink()


def ifdh_cp(fname: pathlib.Path, dest_base: Optional[pathlib.Path]=None, dest_is_dir: bool=True, relative_to: Optional[pathlib.Path]=None):
    dest = dest_base
    if dest_is_dir:
        dest = dest_path(fname, dest_base, relative_to)

    return IFDH_Client.cp([str(fname), str(dest)])


def _transfer_callback(_queue: multiprocessing.Queue, dest: pathlib.Path, relative_to: pathlib.Path, delete: bool=False):
    time.sleep(5)
    pid = multiprocessing.current_process().name
    logger.info(f'Transfer process {pid=} start')

    while True:
        try:
            item = _queue.get(timeout=30)
        except queue.Empty:
            break

        if item == HEARTBEAT:
            logger.debug(f"{pid=} got heartbeat")
            continue

        result = ifdh_cp(item, dest, relative_to=relative_to)

        if result != 0 and result != 17:
            continue

        logger.info(f"{pid=} transfer of {item} finished ({result=})")
        if not delete:
            continue 

        meta_filename = metadata_file(item)
        logger.info(f"{pid=} Removing {item} and metadata file {meta_filename}")
        try:
            item.unlink()
        except PermissionError:
            logger.warning(f"{pid=} Removing {item} failed: Permission denied")

        try:
            meta_filename.unlink()
        except FileNotFoundError:
            logger.warning(f"{pid=} Could not remove metadata file {meta_filename}, not found")
        except PermissionError:
            logger.warning(f"{pid=} Removing {meta_filename} failed: Permission denied")
    
    logger.info(f'Transfer process {pid=} end')


def _declare_callback(_file_queue: multiprocessing.Queue, _declare_queue: multiprocessing.Queue, dest: pathlib.Path, relative_to: pathlib.Path, validate=False, delete=False):
    """
    Callback function for each process. Wraps getting files from a common queue
    shared between processes with file declaration.
    """
    # random sleeps ensures the process declarations are somewhat spread out
    # first random sleep spreads out the processes 
    time.sleep(random.uniform(0, 5))
    pid = multiprocessing.current_process().name
    logger.info(f'Declare process {pid=} start')

    ndeclared = 0
    nskip = 0
    while True:
        try:
            item = _file_queue.get(timeout=10)
        except queue.Empty:
            break

        logger.debug(f'{pid=} Got {item}')
        now = datetime.now()

        loc = dest_path(item, dest, relative_to).parent
        try:
            declare_file(item, loc, validate, delete)
            logger.info(f'{pid=} Declared {item}.')
            ndeclared += 1
            is_virtual = is_virtual_file(item)
            if not is_virtual:
                _declare_queue.put(item)
            else:
                logger.info(f'NOT transferring stage1 file {item}')
        except samweb_client.exceptions.FileAlreadyExists:
            logger.warning(f'{pid=} Skipping {item}, already declared.')
            # even if already declared, file may not be transferred, so queue it anyway
            if not is_virtual:
                _declare_queue.put(item)
            else:
                logger.info(f'NOT transferring stage1 file {item}')
            nskip += 1
        except MetadataNotFoundException:
            logger.warning(f'{pid=} Skipping {item}, metadata not found.')
            _declare_queue.put(HEARTBEAT)
            nskip += 1
        except samweb_client.exceptions.InvalidMetadata as e:
            logger.warning(f'{pid=} Skipping {item}, metadata invalid ({e}).')
            _declare_queue.put(HEARTBEAT)
            nskip += 1
        except FileNotFoundError as e:
            logger.warning(f'{pid=} Skipping {item}, file not found ({e}).')
            _declare_queue.put(HEARTBEAT)
            nskip += 1

        last_request = datetime.now()
        dt = (last_request - now).total_seconds()

        wait = (1.0 / SAM_RPS_MAX) - dt

        # rate limit
        if wait > 0:
            # once again, add a little variance
            wait *= random.uniform(1, SMEAR_MAX)
            logger.debug(f'{pid=} Sleeping {wait:0.4f}s')
            time.sleep(wait)

    # _result.put((ndeclared, nskip))
    logger.info(f'Declare process {pid=} end')


def main(args: dict) -> None:
    filename = pathlib.Path(args.filename)
    dest = pathlib.Path(args.destination)
    if not args.recursive:
        declare_file(filename, validate=args.validate, delete=args.delete)
        ifdh_cp(filename, dest, delete=args.delete)
        sys.exit(0)

    # recursive case, use mutlithreading
    if not filename.is_dir():
        raise RuntimeError(f'Recusive mode requested but {filename} is not a directory.')

    tstart = datetime.now()
    # reco2/stage1: declare json file, don't transfer the file
    # files = filename.rglob('*json')
    files = filename.rglob('*[!json]')
    nprocesses = 0

    # file counter only used to limit spawning new processes. Wait for NFILES_MIN files before spawning
    nfiles_reset = 0
    nfiles = 0

    # files on the system
    file_queue = multiprocessing.Queue()

    # files that have been declared
    declare_queue = multiprocessing.Queue()

    # info from workers
    # result_queue = multiprocessing.Queue()
    declare_processes = []
    transfer_processes = []
    for f in files:
        if not f.is_file():
            continue
        if f.name.startswith('Supplemental'):
            continue
        logger.debug(f'adding {f}')
        stage1_fname = f.with_suffix('')
        nfiles_reset += 1
        nfiles += 1
        file_queue.put(f)

        # for reco2/stage1
        # file_queue.put(stage1_fname)

        # spawn a new process for files until we reach the max number
        # of processes. New process is only spawned once there are at least
        # NFILES_MIN files (fewer processes for small batches of O(10) files)
        # allow transfer process to handle deleting the file & its metadata file
        # so pass false to the delcare process
        if nfiles_reset > NFILES_MIN or len(declare_processes) == 0:
            if len(declare_processes) < DECLARE_NPROCESS_MAX:
                logger.info(f'spawning declaration process')
                t = multiprocessing.Process(target=_declare_callback,
                        args=(file_queue, declare_queue, dest, filename, args.validate, False))
                declare_processes.append(t)
                t.start()
                
            if len(transfer_processes) < TRANSFER_NPROCESS_MAX:
                logger.info(f'spawning transfer process')
                s = multiprocessing.Process(target=_transfer_callback,
                        args=(declare_queue, dest, filename, args.delete))
                transfer_processes.append(s)
                s.start()

            nfiles_reset = 0


    for p in declare_processes:
        p.join()

    for p in transfer_processes:
        p.join()
    '''
    ndeclared = 0
    nskip = 0
    for p in processes:
        p.join()
    tend = datetime.now()

    while True:
        try:
            nd, ns = result_queue.get(block=False)
            ndeclared += nd
            nskip += ns
        except queue.Empty:
            break

    logger.info('Done')
    dt = (tend - tstart).total_seconds()
    logger.info(f'Processed {nfiles} files (declared={ndeclared}, skip={nskip}) in {dt:.2f} seconds ({(nskip + ndeclared) / dt:.2f} files per second)')
    '''


if __name__ == '__main__':
    formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(filename='sam_declare.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)

    parser = argparse.ArgumentParser(
        prog='sam_declare',
        description='Declare files to SAM, adding common metadata fields and file locations.')
    parser.add_argument('filename')
    parser.add_argument('destination')
    parser.add_argument('-R', '--recursive', action='store_true', help='If filename is a directory, glob for files within.')
    parser.add_argument('-V', '--validate', action='store_true', help='Validate metadata before declaring')
    parser.add_argument('-d', '--delete', action='store_true', help='Remove file & json file after declaration and transfer')
    # parser.add_argument('-s', '--scan', action='store_true', help='With -R, periodically rescan directory for new files.')
    # parser.add_argument('-p', '--pattern', help='Match filenames by a pattern. Can speed up scanning directories with lots of files.')
    args = parser.parse_args()

    main(args)
