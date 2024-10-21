"""
Monitoring functions
"""

import time
import json
import collections
import subprocess
import threading
import pathlib
from datetime import datetime
from typing import Optional

import logging
logger = logging.getLogger(__name__)


def ifstat():
    """Return the output of ifstat --json, and unwrap the kernel dict."""
    return json.loads(subprocess.check_output(['ifstat', '--json']))['kernel']

def ip(interface: str):
    """Return the output of ip command as JSON."""
    args = ('ip', '-s', '--json', 'link', 'show', 'dev', interface)
    return json.loads(subprocess.check_output(args))[0]['stats64']


class NetworkMonitor:
    """
    Class to sample network receive and transfer bytes at regular intervals.
    Args:
        interface: Network interface name
        sample_rate: Readings are taken at this rate
        history_size: Data will only be available for the past history_size readings
    """
    def __init__(self, interface: str, sample_rate: int, fname: Optional[pathlib.Path]=None, history_size: int=1000):
        self.interface = interface
        self.sample_rate = sample_rate
        self.data = collections.deque(maxlen=history_size)
        self.output_filename = fname
        if fname is not None:
            logger.info(f'Writing NetworkMonitor output to {fname}')
        else:
            logger.info(f'Writing NetworkMonitor output to stdout')

        self.sample()
        self._thread = threading.Thread(target=self._callback, daemon=True)
        self._stop = False
        self._first = True

    def start(self):
        self._stop = False
        self._first = True
        self._thread.start()

    def stop(self):
        self._stop = True

    def sample(self):
        """Make a measurement."""
        self._last_sample_time = datetime.now()
        self._last_sample = ip(self.interface)['tx']['bytes']
        logger.debug(f'Network monitor sample call at {self._last_sample_time} ({self._last_sample} bytes)')
        return self._last_sample_time, self._last_sample

    def _callback(self):
        """Difference between last measurement and this one."""
        # copy last measurement
        while not self._stop:
            logger.debug(f'Network monitor for {self.interface} starting')
            then = self._last_sample_time
            total_bytes = self._last_sample

            # update measurement
            now, now_sample = self.sample()
            total_bytes = now_sample - total_bytes

            elapsed_s = (now - then).total_seconds()

            # try to catch up
            excess_s = elapsed_s - self.sample_rate
            wait_s = self.sample_rate
            if excess_s > 0 and excess_s < wait_s:
                wait_s -= excess_s

            # skip the first measurement (dt~=0)
            if not self._first:
                self.data.append((now, elapsed_s, total_bytes))
                str_ = f"{now.strftime('%Y-%m-%d %H:%M:%S')}, {elapsed_s:.2f}, {total_bytes:d}\n"
                print(str_[:-1])
                if self.output_filename is not None:
                    with open(self.output_filename, 'a') as f:
                        f.write(str_)
            else:
                self._first = False

            logger.debug(f'Network monitor for {self.interface} waiting {wait_s:.1f} seconds')
            print(f'Network monitor for {self.interface} waiting {wait_s:.1f} seconds')
            time.sleep(wait_s)

    def reset(self):
        self.data.clear()


if __name__ == '__main__':
    nt = NetworkMonitor('eno1', 5)
    nt.start()
    time.sleep(15)
    nt.stop()
    print(nt.data)
