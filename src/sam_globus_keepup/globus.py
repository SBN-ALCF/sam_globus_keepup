"""
GLOBUS helpers
References:
- https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#best-effort-proactive-handling-of-consentrequired
- https://globus-sdk-python.readthedocs.io/en/stable/tutorial.html
"""


import os
import pathlib
from typing import List

import globus_sdk
from globus_sdk.scopes import TransferScopes

import logging
logger = logging.getLogger(__name__)


CONSENT_REQ_ERR_MSG = "Encountered a ConsentRequired error: You must login a second time to grant consents."


class GLOBUSSessionManager:
    """ContextManager for GLOBUS transfers."""
    def __init__(self, client_id: str, src_endpoint: str, dest_endpoint: str):
        logger.info('Initializing GLOBUS manager.')

        self.auth_client = globus_sdk.NativeAppAuthClient(client_id)
        self.client = None
        self.src_endpoint = src_endpoint
        self.dest_endpoint = dest_endpoint
        self.token_data = {}
        self._task_data = None
        self._last_task_id = None

        logger.info('f{self.src_endpoint=} {self.dest_endpoint=}')


    def __enter__(self):
        logger.debug('Trying to establish transfer client')
        self.client = self._get_transfer_client()

        # check if we need additional scopes
        consent_required_scopes = []
        consent_required_scopes.extend(self._required_scopes(self.src_endpoint))
        consent_required_scopes.extend(self._required_scopes(self.dest_endpoint))
        if consent_required_scopes:
            logger.debug(f'Requesting additional scopes: {consent_required_scopes}')
            self.client = self._get_transfer_client(scopes=consent_required_scopes)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        logger.info('Bye.')

    def _get_transfer_client(self, scopes=TransferScopes.all):
        """Get tokens via web authentication, then read token data to construct
        transfer client with proper authorization."""
        self.auth_client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)

        authorize_url = self.auth_client.oauth2_get_authorize_url()
        print(f"Please go to this URL and login:\n\n{authorize_url}\n")
        auth_code = input("Please enter the code here: ").strip()

        tokens = self.auth_client.oauth2_exchange_code_for_tokens(auth_code)

        self.token_data = tokens.by_resource_server["transfer.api.globus.org"]

        transfer_rt = self.token_data["refresh_token"]
        transfer_at = self.token_data["access_token"]
        expires_at_s = self.token_data["expires_at_seconds"]

        authorizer = globus_sdk.RefreshTokenAuthorizer(
            transfer_rt, self.auth_client, access_token=transfer_at, expires_at=expires_at_s
        )

        return globus_sdk.TransferClient(authorizer=authorizer)

    def _required_scopes(self, target: str) -> List:
        """Try to perform an `ls` of the endpoint to check which scopes are needed to access it."""
        try:
            self.client.operation_ls(target, path="/")
        except globus_sdk.TransferAPIError as err:
            if err.info.consent_required:
                return err.info.consent_required.required_scopes
        
        return []

    def add_manifest(self, manifest_filename: str) -> None:
        """Add files from a manifest with SRC and DEST for each file on a
        separate line, similar to the GLOBUS CLI"""
        with open(manifest_filename, 'r') as f:
            for line in f.readlines():
                src, dest = line.split()
                self.add_file(pathlib.PurePosixPath(src), pathlib.PurePosixPath(dest))

    def add_file(self, file_src: pathlib.PurePosixPath, file_dest: pathlib.PurePosixPath) -> None:
        """Add file to transfer to the current task."""

        # start a new task if we don't have one yet
        if self._task_data is None:
            self._task_data = globus_sdk.TransferData(source_endpoint=self.src_endpoint, destination_endpoint=self.dest_endpoint)

        self._task_data.add_item(str(file_src), str(file_dest))

    def clear_task(self) -> None:
        """Reset task data. Currently this just clears the reference."""
        if self._task_data is None:
            return

        self._task_data = None

    def submit(self) -> str:
        if self._task_data is None:
            logger.warn('Called submit_task with no task data, skipping.')
            return

        # this can fail in rare cases. Solution is to renew the client
        try:
            task_doc = self.client.submit_transfer(self._task_data)
        except globus_sdk.TransferAPIError as err:
            if not err.info.consent_required:
                raise err

            logger.warning(CONSENT_REQ_ERR_MSG)
            print(CONSENT_REQ_ERR_MSG)
            self.client = self._get_transfer_client(scopes=err.info.consent_required.required_scopes)
            task_doc = self.client.submit_transfer(self._task_data)

        task_id = task_doc["task_id"]
        logger.info(f"Submitted transfer, task_id={task_id}")
        self.clear_task()
        self._last_task_id = task_id
        return task_id

    def wait(self, task_id=None):
        """Sleep until task is completed. If no task ID, use the last submission ID."""
        if task_id is None and self._last_task_id is None:
            logger.warning('Tried to wait on a task but the task ID was not specified and there was no last task.')

        if task_id is None:
            task_id = self._last_task_id

        logger.info(f"Waiting on {task_id=}")
        while not self.client.task_wait(task_id, timeout=60):
            logger.info(f"Waiting on {task_id=}")

    @property
    def task_nfiles(self):
        if self._task_data is None:
            return 0
        return len(list(self._task_data.iter_items()))
