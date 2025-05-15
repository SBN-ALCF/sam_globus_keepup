"""
SAM helpers
"""

import sys
import os
import time
import pathlib
# import threading
import queue
import requests
import multiprocessing
from datetime import datetime
from typing import Optional

import samweb_client

import logging
logger = logging.getLogger(__name__)

from . import IFDH_Client, SAMWeb_Client, EXPERIMENT
from sam_globus_keepup.ifdhmanager import IFDHProjectManager


API_URL = 'https://fndca3b.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr'


def SAM_dataset_exists(dataset: str) -> bool:
    try:
        SAMWeb_Client.descDefinition(dataset)
        return True
    except samweb_client.exceptions.DefinitionNotFound:
        return False


def file_locality(fname: str) -> str:
    """Look up file with SAM and return its file locality."""
    # kind of ugly, need to catch if we get a path instead of a name, but can't
    # use pathlib since string might have a protocol prefix, e.g., root:///.
    if '/' in fname:
        fname = fname.split('/')[-1]

    file_path_str = SAMWeb_Client.getFileAccessUrls(fname, schema='file')[0]
    print(file_path_str)
    if not file_path_str:
        raise RuntimeError(f'Could not get file locality for {fname}. Is it declared to SAM?')

    # extract URL from xroot string
    path = pathlib.PurePath(file_path_str.split('file://')[1]).relative_to('/pnfs')
    request_url = f'{API_URL}/{path}?locality=True'

    # need to set verify=False
    result = requests.get(request_url, verify=False)

    if result.status_code != 200:
        raise RuntimeError(f'Could not get file locality for {fname}, invalid API response.')

    return result.json()['fileLocality']


def ifdh_cp(self, fname: str, dest: Optional[pathlib.Path]=None, dest_is_dir: bool=True):
    """Alias for self._client.cp(self._current_file, dest)."""
    if dest is None:
        dest = pathlib.Path().resolve()

    if not fname:
        raise RuntimeError('Tried to save empty file.')

    # if the destination is not a file, create the directory first
    if dest_is_dir:
        dest.mkdir(parents=True, exist_ok=True)

    IFDH_Client.cp([fname, str(dest)])


class SAMProjectManager(IFDHProjectManager):
    """ContextManager for running a SAM project."""
    def __init__(self, sam_user, project_base: str, dataset: str, parallel: int=1):
        super().__init__(project_base, parallel)
        self._sam_user = sam_user
        self._samweb_client = SAMWeb_Client

        # take a snapshot of the definition to fix the file list.
        # The direct approach, "defname: ... minus ...", query takes a very long time
        snap_id = self._samweb_client.takeSnapshot(dataset)
        self.dims = f"(snapshot_id {snap_id} minus ((project_name like {project_base}_% and consumed_status 'consumed')))"

        self.dataset = f"{project_base}_{dataset}_TEST"

        # check if dataset exists, delete it if it does
        if SAM_dataset_exists(self.dataset):
            self._samweb_client.deleteDefinition(self.dataset)
        self._samweb_client.createDefinition(self.dataset, self.dims)

        self._url = None
        self._cpid = None

    def _count_files(self):
        return self._samweb_client.countFiles(self.dims)

    def _project_start(self):
        # name, station, dataset, user, group
        _url = self._client.startProject(self.project_name, EXPERIMENT, self.dataset, self._sam_user, EXPERIMENT)
        time.sleep(2)
        self._url = self._client.findProject(self.project_name, EXPERIMENT)
        logger.info(f"Project started with {self._url=}")

    def _project_end(self):
        self._client.endProject(self._url)
        self._client.cleanup()
        time.sleep(1)
        snap_id = self._samweb_client.projectSummary(self._url)['snapshot_id']
        logger.info(f'finished with {snap_id=}')

    def _threaded_process_next(self, callback, check_locality=False) -> None:
        """
        Get the next file from the SAM project and run callback(file)
        Done in a thread for each parallel process. Once complete,
        file is added to this object's queue.
        """
        # url, appname, appversion, dest, user
        process_id = self._client.establishProcess(self._url, "dummy", "dummy", "dummy", self._sam_user) #, schemas="http")
        while True:
            next_file = self._client.getNextFile(self._url, process_id)
            if not next_file:
                break

            if check_locality:
                if not 'ONLINE' in file_locality(next_file):
                    logger.warning(f'File {next_file} is not found on disk. Skipping.')
                    continue

            # do something with file
            if callback is not None:
                callback(next_file)

            self.release_file(next_file, process_id)
            logger.debug(f'transferred & released file ({next_file=})')
            self._done_queue.put(next_file)
            try:
                logger.debug(f'Approximate queue length (qsize) is {self._done_queue.qsize()}')
            except NotImplementedError:
                pass

        logger.debug(f'getNextFile empty, ending process')

    def release_file(self, fname: str, process_id: int):
        """Mark a file as completed within this project."""
        self._client.updateFileStatus(self._url, process_id, fname, "transferred")
        time.sleep(0.5)
        self._client.updateFileStatus(self._url, process_id, fname, "consumed")

    def running(self) -> bool:
        """Override to check url."""
        if self._url is None:
            return False

        for t in self._processes:
            if t.is_alive():
                return True

        # all processes ended
        return False

        # return self._samweb_client.projectSummary(self._url)['project_status'] == 'running'
