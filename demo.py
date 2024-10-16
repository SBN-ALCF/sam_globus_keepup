#!/usr/bin/env python

"""
Copies new files from dCache to this node using ifdh, then sends them to
Polaris at ALCF via GLOBUS
"""

import sys
import os
import re
import time
import pathlib
import threading
import subprocess
from datetime import datetime
from typing import Optional

import ifdh
import samweb_client

import globus_sdk

import logging
logger = logging.getLogger(__name__)


DATASET = "sbnd_keepup_from_17200_raw_Oct14"
EXPERIMENT = None


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


def run_path(run_number: int):
    """Convert 8-digit int ABCXYZPQ to path AB/CX/YZ/PQ."""
    s = f'{run_number:08d}'
    return pathlib.Path(*[s[i:i+2] for i in range(0, len(s), 2)])


def du(path: pathlib.Path) -> int:
    """Return the output of du -s."""
    if not path.exists():
        raise RuntimeError(f'Path {path} does not exist.')

    return int(subprocess.check_output(['du','-sx', path]).split()[0])


def main(project_base: str, output_path: pathlib.Path):

    run_regexp = re.compile(".*data_evb(\d+)_.*run(\d+)_.*.root")
    # rawdata_path = pathlib.Path('/scratch/sbnd/caf')
    
    # 10 GB
    buffer_kb = 10000000

    # sam transfer
    with SAMProjectManager(project_base=project_base, dataset=DATASET) as client:
        for f in client.get_files():
            logger.info(f"Starting transfer of {f}")
            result = run_regexp.match(f)
            evb, run_number = (int(g) for g in result.groups())
            outdir = output_path / run_path(run_number)
            client.save_file(dest=outdir)

        # globus transfer
        # TODO


if __name__ == "__main__":
    if "EXPERIMENT" not in os.environ:
        raise RuntimeError("Must set EXPERIMENT environment variable.")
    if "IFDH_PROXY_ENABLE" not in os.environ:
        raise RuntimeError("Must set IFDH_PROXY_ENABLE=0 environment variable.")

    EXPERIMENT = os.environ["EXPERIMENT"]
    output_path = pathlib.Path('/scratch/sbnd/rawdata')

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main(project_base='globus_dtn_xfer_test', output_path=output_path)
