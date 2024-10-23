"""
SAM helpers
"""

import sys
import os
import time
import pathlib
import threading
import queue
from datetime import datetime
from typing import Optional

import samweb_client

import logging
logger = logging.getLogger(__name__)

from . import IFDH_Client, SAMWeb_Client, EXPERIMENT


def SAM_dataset_exists(dataset: str) -> bool:
    try:
        SAMWeb_Client.descDefinition(dataset)
        return True
    except samweb_client.exceptions.DefinitionNotFound:
        return False


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


class SAMProjectManager:
    """ContextManager for running a SAM project."""
    def __init__(self, project_base: str, dataset: str, parallel: int=1):
        self._client = IFDH_Client
        self._samweb_client = SAMWeb_Client
        self._parallel = parallel
        self._process_ids = [None] * self._parallel

        now_str = datetime.now().strftime("%Y%m%dT%H%M%S")
        project_name = f"{project_base}_{now_str}"
        self.project_name = project_name
        self.dims = f"(defname: {dataset} minus ((project_name like {project_base}_% and consumed_status like 'consumed')))"

        self.dataset = f"{project_base}_{dataset}_TEST"

        # check if dataset exists, create it if not
        if not SAM_dataset_exists(self.dataset):
            self._samweb_client.createDefinition(self.dataset, self.dims)

        self.nfiles = self._samweb_client.countFiles(self.dims)

        logger.info(f"Starting project for definition {self.dataset}, dims={self.dims}")
        logger.info(f"{self.nfiles=}")

        self._url = None
        self._cpid = None

        # if there is at least one file, will be overriden via getNextFile
        self._current_file = 'dummy_first'
        if self.nfiles == 0:
            self._current_file = None 

        # we use threaded getNextFile calls, but user may want a serial output of files
        self._queue = queue.Queue()
        self._threads = []

    def __enter__(self):
        logger.info("Project starting...")
        if self.nfiles == 0:
            # do nothing, since we cannot start a project with no files
            logger.info(f"No files in dataset {self.dataset}, project will not be created.")
            return self

        # name, station, dataset, user, group
        _url = self._client.startProject(self.project_name, EXPERIMENT, self.dataset, "sbndpro", EXPERIMENT)
        time.sleep(2)
        self._url = self._client.findProject(self.project_name, EXPERIMENT)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for t in self._threads:
            t.join()

        logger.info("Project ending...")
        if self.nfiles == 0:
            # no project to end if there were no files
            return
        
        self._client.endProject(self._url)
        self._client.cleanup()
        time.sleep(1)
        snap_id = self._samweb_client.projectSummary(self._url)['snapshot_id']
        logger.info(f'finished with {snap_id=}')

    def start(self, callback=None):
        """Start processes for copying files. Once they are copied, add them to our queue."""
        self._threads = []
        for i in range(self._parallel):
             t = threading.Thread(target=self._threaded_copy, args=(callback,))
             self._threads.append(t)
             t.start()
            
    def _threaded_copy(self, callback) -> None:
        """Done in a thread for each parallel process."""
        # url, appname, appversion, dest, user
        process_id = self._client.establishProcess(self._url, "dummy", "dummy", "dummy", "sbndpro")
        while True:
            next_file = self._client.getNextFile(self._url, process_id)
            if not next_file:
                break

            # do something with file
            if callback is not None:
                callback(next_file)

            self.release_file(next_file, process_id)
            self._queue.put(next_file)

    def release_file(self, fname: str, process_id: int):
        """Mark a file as completed within this project."""
        self._client.updateFileStatus(self._url, process_id, fname, "transferred")
        time.sleep(0.5)
        self._client.updateFileStatus(self._url, process_id, fname, "consumed")

    def get_files(self) -> str:
        """Generator wrapper for getNextFile."""
        while not self._queue.empty():
            self._current_file = self._queue.get()
            yield self._current_file
