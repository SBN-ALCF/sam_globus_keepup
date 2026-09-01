"""
GLOBUS helpers
References:
- https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#best-effort-proactive-handling-of-consentrequired
- https://globus-sdk-python.readthedocs.io/en/stable/tutorial.html
"""


import os
import copy
import pathlib
import threading
from typing import List

import globus_sdk
from globus_sdk.scopes import TransferScopes
from globus_sdk.gare import GlobusAuthorizationParameters

from sam_globus_keepup.utils import check_env

import logging
logger = logging.getLogger(__name__)


CONSENT_REQ_ERR_MSG = "Encountered a ConsentRequired error: You must login a second time to grant consents."

# how long wait() will block for the submission thread to report a task ID
SUBMIT_TIMEOUT_S = 600

# Poll interval and overall ceiling for a single transfer task. GLOBUS retries
# an unreadable source path instead of failing it, so without a ceiling a task
# that references one missing file never reaches a terminal state and the
# transfer manager waits on it forever. TASK_TIMEOUT_S must comfortably exceed
# how long a full GLOBUS_NFILE_MAX batch takes on your link.
TASK_POLL_S = 60
TASK_TIMEOUT_S = 2 * 3600


class GLOBUSSessionManager:
    """ContextManager for GLOBUS transfers."""
    def __init__(self, client_id: str, src_endpoint: str, dest_endpoint: str):
        logger.info('Initializing GLOBUS manager.')

        try:
            self._client_id = check_env("GLOBUS_API_CLIENT_ID")
            logger.info(f'Using client ID {self._client_id} from GLOBUS_API_CLIENT_ID environment variable.')
        except RuntimeError:
            self._client_id = client_id

        # does the user have a secret set? If so, try to use services account
        self._use_services = False
        try:
            self._client_secret = check_env("GLOBUS_APP_SECRET")
            self._use_services = True
            logger.info('GLOBUS_APP_SECRET is set, will use service account authorization')
        except RuntimeError:
            logger.warning('Could not get GLOBUS_APP_SECRET from environment.')
            logger.warning('Falling back to native app authorization')

        self.auth_client = None
        if not self._use_services:
            # auth_client is only needed for native app authorization, i.e., not
            # needed for a services account (see _get_transfer_client method)
            self.auth_client = globus_sdk.NativeAppAuthClient(client_id)

        self.client = None
        self.src_endpoint = src_endpoint
        self.dest_endpoint = dest_endpoint
        self.token_data = {}
        self._task_data = None
        self._rm_task_data = None
        self._rm_list = []
        self._batch_keys = set()
        self._last_task_id = None
        self._thread = None
        self._running = False
        # set once the submission thread has a task ID to report
        self._submitted = threading.Event()

        logger.info(f'{self.src_endpoint=} {self.dest_endpoint=}')


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

        if self._use_services:
            app = globus_sdk.ClientApp(
                "SBND Keepup Transfer", client_id=self._client_id, client_secret=self._client_secret,
            )

            return globus_sdk.TransferClient(app=app, app_scopes=scopes)


        # Native app authorization
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

    def ls(self, endpoint=None, path=None) -> None:
        """User-visible way to check the endpoint"""
        if endpoint is None:
            endpoint = self.dest_endpoint
        if path is None:
            path = "/"

        # support posixpath from pathlib
        if not isinstance(path, str):
            path = str(path)

        return self.client.operation_ls(endpoint, path=path)

    def add_manifest(self, manifest_filename: str) -> None:
        """Add files from a manifest with SRC and DEST for each file on a
        separate line, similar to the GLOBUS CLI"""
        with open(manifest_filename, 'r') as f:
            for line in f.readlines():
                src, dest = line.split()
                self.add_file(pathlib.PurePosixPath(src), pathlib.PurePosixPath(dest))

    def add_file(self, file_src: pathlib.PurePosixPath, file_dest: pathlib.PurePosixPath) -> bool:
        """Add file to transfer to the current task.

        Returns True if the file was added. A source that is missing, empty or
        already queued is rejected here: GLOBUS retries an unreadable source
        path rather than failing it, so a single bad entry stalls the whole
        task and every file batched behind it.
        """
        # keep a concrete Path so the existence check and the later unlink work
        src = pathlib.Path(file_src)

        # This is reachable whenever a leftover under an older scratch layout
        # is also re-delivered by SAM into the current one.
        if src.name in self._batch_keys:
            logger.warning(f'Not adding {src}: {src.name} is already queued in this task.')
            return False

        try:
            if not src.is_file():
                logger.warning(f'Not adding {src}: source is missing.')
                return False
            if src.stat().st_size == 0:
                logger.warning(f'Not adding {src}: source is empty.')
                return False
        except OSError as e:
            logger.warning(f'Not adding {src}: could not stat source ({e}).')
            return False

        # start a new task if we don't have one yet
        if self._task_data is None:
            self._task_data = globus_sdk.TransferData(source_endpoint=self.src_endpoint, destination_endpoint=self.dest_endpoint)
            # kept in sync with _task_data for a possible future server-side
            # delete; cleanup currently unlinks locally in _submit_and_clean
            self._rm_task_data = globus_sdk.DeleteData(endpoint=self.src_endpoint)

        self._task_data.add_item(str(src), str(file_dest))
        self._rm_task_data.add_item(str(src))
        self._rm_list.append(src)
        self._batch_keys.add(src.name)
        return True

    def clear_task(self) -> None:
        """Reset task data. Currently this just clears the reference."""
        if self._task_data is None:
            return

        self._task_data = None
        self._rm_task_data = None
        self._rm_list = []
        self._batch_keys = set()

    def submit(self) -> str:
        if self._task_data is None:
            logger.warn('Called submit_task with no task data, skipping.')
            return

        if self._thread is not None:
            if self._thread.is_alive():
                logger.warn('Called submit_task while a previous was still running. Waiting...')
                self._thread.join()

        # copy task data, then clear. As soon as we submit, want to be able to
        # start setting up next task
        task_data = copy.copy(self._task_data)
        rm_task_data = copy.copy(self._rm_task_data)
        rm_list = copy.copy(self._rm_list)
        self.clear_task()

        self._running = True
        self._submitted.clear()
        self._last_task_id = None

        self._thread = threading.Thread(target=self._threaded_submit, args=(task_data, rm_task_data, rm_list))
        self._thread.start()

    def _threaded_submit(self, task_data, rm_task_data, rm_list):
        """Wrapper that guarantees the running flag is released."""
        try:
            self._submit_and_clean(task_data, rm_task_data, rm_list)
        except Exception:
            # an unhandled exception in a bare thread is invisible, and the
            # batch has already been cleared from _task_data by submit()
            logger.exception(f'Transfer thread failed. {len(rm_list)} files left on scratch.')
        finally:
            # Must always run. Returning with _running still set stalls the
            # main loop permanently and files pile up on scratch.
            self._running = False
            self._submitted.set()

    def _submit_and_clean(self, task_data, rm_task_data, rm_list):
        """Do submission in a thread so we can wait between transfer & cleanup."""

        # this can fail in rare cases. Solution is to renew the client
        try:
            task_doc = self.client.submit_transfer(task_data)
        except globus_sdk.TransferAPIError as err:
            if not err.info.consent_required:
                raise err

            if not self._use_services:
                # Native app authorization prompts on stdin (see
                # _get_transfer_client). Calling that from this worker thread
                # blocks it forever!
                logger.critical(CONSENT_REQ_ERR_MSG)
                raise RuntimeError(CONSENT_REQ_ERR_MSG) from err

            # a services account renews without any console interaction
            logger.warning(CONSENT_REQ_ERR_MSG)
            self.client = self._get_transfer_client(scopes=err.info.consent_required.required_scopes)
            task_doc = self.client.submit_transfer(task_data)
        
        task_id = task_doc["task_id"]
        self._last_task_id = task_id
        self._submitted.set()
        logger.info(f"Submitted transfer, task_id={task_id}")
        self.wait(task_id=task_id)

        task = self.client.get_task(task_id)
        status = task['status']
        logger.info(f"Transfer task with {task_id=} ended with status {status}. Cleaning up...")

        # Delete only what GLOBUS confirms it moved, rather than trusting the
        # task status directly.  However, the per-file list is paginated, which
        # costs one API call per page and makes each page a chance to fail. The
        # task document already reports how many files moved, so when that
        # count accounts for the whole batch we can skip the pages.
        n_files = len(rm_list)
        n_transferred = task.get('files_transferred')
        n_skipped = (task.get('files_skipped') or 0) + (task.get('subtasks_skipped_errors') or 0)

        if status == 'SUCCEEDED' and n_transferred == n_files and n_skipped == 0:
            logger.info(f'{task_id} moved all {n_files} files; cleaning up without enumerating.')
            transferred = {str(f) for f in rm_list}
        else:
            logger.info(
                f'{task_id}: reports {n_transferred} of {n_files} transferred, {n_skipped} '
                f'skipped, status {status}. Enumerating to find which files moved.'
            )
            # Must go through .paginated
            transferred = {
                item["source_path"]
                for item in self.client.paginated.task_successful_transfers(task_id).items()
            }

            # If the enumeration disagrees with the task's own count then the
            # list is incomplete and we cannot tell which files are safe to
            # remove. Leave the whole batch for the next run rather than guess.
            if n_transferred is not None and len(transferred) != n_transferred:
                logger.error(
                    f'{task_id}: enumerated {len(transferred)} transfers but the task reports '
                    f'{n_transferred}. Deleting nothing; {n_files} files stay on scratch.'
                )
                return

        n_left = 0
        for f in rm_list:
            if str(f) not in transferred:
                n_left += 1
                logger.warning(
                    f'{f} was not confirmed transferred by {task_id}; leaving it on scratch.'
                )
                continue
            try:
                f.unlink()
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning(f'{e}')

        if n_left:
            logger.warning(f'{n_left}/{len(rm_list)} files from {task_id} remain on scratch.')

    def wait(self, task_id=None):
        """Sleep until task is completed. If no task ID, use the last submission ID."""
        if task_id is None and self._last_task_id is None:
            logger.warning('Tried to wait on a task but the task ID was not specified and there was no last task.')
            return

        if task_id is None:
            # The ID is assigned by the submission thread. Without this the
            # caller reads None and returns immediately
            if not self._submitted.wait(timeout=SUBMIT_TIMEOUT_S):
                logger.warning(f'Timed out after {SUBMIT_TIMEOUT_S}s waiting for a task ID.')
            task_id = self._last_task_id
            if task_id is None:
                logger.warning('Submission did not produce a task ID; nothing to wait on.')
                return

        logger.info(f"Waiting on {task_id=}")
        waited = 0
        while not self.client.task_wait(task_id, timeout=TASK_POLL_S):
            waited += TASK_POLL_S
            logger.info(f"Waiting on {task_id=} ({waited}s elapsed)")
            if waited >= TASK_TIMEOUT_S:
                logger.error(
                    f"Task {task_id} did not finish within {TASK_TIMEOUT_S}s. Cancelling it; "
                    "check the GLOBUS console for per-file errors."
                )
                self.client.cancel_task(task_id)
                return

    def running(self):
        return self._running

        '''
        # legacy implementation. Might be useful
        if self._last_task_id is None:
            return False

        # check if the task is running by calling task_wait with 1s timeout
        task = self.client.task_list(filter={'task_id': self._last_task_id})['DATA'][0]
        return 'ACTIVE' in task['status']
        '''

    @property
    def task_nfiles(self):
        if self._task_data is None:
            return 0
        return len(list(self._task_data.iter_items()))
