"""
SAM helpers
"""

import sys
import os
import time
import pathlib
import threading
from datetime import datetime
from typing import Optional

import ifdh
import samweb_client

import logging
logger = logging.getLogger(__name__)


class SAMProjectManager:
    """ContextManager for running a SAM project."""
    def __init__(self, project_base: str, dataset: str):
        self._client = ifdh.ifdh()
        self._samweb_client = samweb_client.SAMWebClient()

        now_str = datetime.now().strftime("%Y%m%dT%H%M%S")
        project_name = f"{project_base}_{now_str}"
        self.project_name = project_name
        self.dims = f"(defname: {dataset} minus ((project_name like {project_base}_% and consumed_status like 'consumed')))"

        self.dataset = f"{project_base}_{dataset}_TEST"

        # check if dataset exists, create it if not
        try:
            self._samweb_client.descDefinition(self.dataset)
        except samweb_client.exceptions.DefinitionNotFound:
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

        # url, appname, appversion, dest, user
        self._process_id = self._client.establishProcess(self._url, "dummy", "dummy", "dummy", "sbndpro")

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        logger.info("Project ending...")
        if self.nfiles == 0:
            # no project to end if there were no files
            return
        
        self._client.endProject(self._url)
        self._client.cleanup()
        time.sleep(1)
        snap_id = self._samweb_client.projectSummary(self._url)['snapshot_id']
        logger.info(f'finished with {snap_id=}')

    def get_files(self):
        """Generator wrapper for getNextFile."""
        while self._current_file:
            self._current_file = self._client.getNextFile(self._url, self._process_id)
            if not self._current_file:
                break

            yield self._current_file

    def save_file(self, fname: Optional[str]=None, dest: Optional[pathlib.Path]=None, dest_is_dir: bool=True, release: bool=True):
        """Alias for self._client.cp(self._current_file, dest)."""
        if fname is None:
            fname = self._current_file

        if dest is None:
            dest = pathlib.Path().resolve()

        if not fname:
            raise RuntimeError('Tried to save empty file.')

        # if the destination is not a file, create the directory first
        if dest_is_dir:
            dest.mkdir(parents=True, exist_ok=True)

        # wrap this in a thread so that ifdh cp doesn't crash the project
        logger.debug(f"Creating thread for transfer of {fname}")
        thread = threading.Thread(target=self._threaded_cp, args=(fname, dest, release))
        thread.start()
        thread.join()
        logger.debug(f"Thread finished")

    def _threaded_cp(self, fname: str, dest: pathlib.Path, release: bool):
        """Do cp in a thread, so that exit signals don't interfere with the main program.
        TODO: The above comment was the intention, but it doesn't seem to work to prevent whole-program exit."""
        self._client.cp([fname, str(dest)])

        if release:
            self.release_current_file()


    def release_current_file(self):
        """Mark the current file as completed."""
        if not self._current_file:
            return

        self._client.updateFileStatus(self._url, self._process_id, self._current_file, "transferred")
        time.sleep(1)
        self._client.updateFileStatus(self._url, self._process_id, self._current_file, "consumed")
