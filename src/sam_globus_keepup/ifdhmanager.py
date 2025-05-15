"""
Base class manager for file transfer to the node using parallel ifdh calls
Override _project_start, _count_files, _threaded_process_next functions in
derived classes

This class implements a multiprocessing queue. As files are copied to the node,
they are added to the queue This allows other services (GLOBUS) to get files
from the queue
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

import logging
logger = logging.getLogger(__name__)

from . import IFDH_Client, SAMWeb_Client, EXPERIMENT

class IFDHProjectManager:
    """ContextManager for file transfer."""
    def __init__(self, project_base: str, parallel: int=1):
        self._client = IFDH_Client
        self._parallel = parallel
        self._process_ids = [None] * self._parallel

        now_str = datetime.now().strftime("%Y%m%dT%H%M%S")
        project_name = f"{project_base}_{now_str}"
        self.project_name = project_name

        self.nfiles = 0
        self._current_file = 'dummy_first'
        self._transfer_queue = multiprocessing.Queue()
        self._done_queue = multiprocessing.Queue()
        self._processes = []

    def _count_files(self):
        return 0

    def _project_start(self):
        pass

    def __enter__(self):
        logger.info(f"Project {self.project_name} starting...")

        self.nfiles = self._count_files()
        if self.nfiles == 0:
            self._current_file = None 

        logger.info(f"{self.nfiles=}")

        if self.nfiles == 0:
            # do nothing, since we cannot start a project with no files
            logger.info(f"No files in dataset {self.dataset}, project will not be created.")
            return self

        self._project_start()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for t in self._processes:
            t.join()

        logger.info("Project ending...")
        if self.nfiles == 0:
            # no project to end if there were no files
            return

        self._project_end()

    def start(self, callback=None, **callback_kwargs):
        """Start processes for copying files. Once they are copied, add them to our queue."""
        if self.nfiles == 0:
            logger.info(f"No files to process, not starting.")
            return False

        self._processes = []
        for i in range(self._parallel):
             t = multiprocessing.Process(target=self._threaded_process_next, args=(callback,), kwargs=callback_kwargs)
             self._processes.append(t)
             t.start()
            
    def _threaded_process_next(self, callback, **kwargs) -> None:
        """Generic threaded function to get next file from the transfer queue,
        then add it to the done queue after a callback."""
        while True:
            try:
                next_file = self._transfer_queue.get(block=False)
            except queue.Empty:
                break

            # do something with file
            if callback is not None:
                callback(next_file)

            logger.debug(f'transferred file ({next_file=})')
            self._done_queue.put(next_file)
            try:
                logger.debug(f'Approximate queue length (qsize) is {self._done_queue.qsize()}')
            except NotImplementedError:
                pass

        logger.debug(f'getNextFile empty, ending process')

    def get_file(self) -> str:
        if self._done_queue.empty():
            return None

        try:
            item = self._queue.get(block=False)
        except queue.Empty:
            logger.debug(f'failed to get file within {timeout} s')
            return None

        return item

    def running(self) -> bool:
        for t in self._processes:
            if t.is_alive():
                return True

        # all processes ended
        return False
