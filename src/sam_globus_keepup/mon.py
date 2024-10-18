"""
Monitoring functions
"""

import datetime
import json
import collections
import subprocess
import statistics

import logging
logger = logging.getLogger(__name__)


def ifstat():
    """Return the output of ifstat --json, and unwrap the kernel dict."""
    return json.loads(subprocess.check_output(['ifstat', '--json']))['kernel']


class NetworkTimer:
    """
    Class to sample network receive and transfer bytes at regular intervals.
    Args:
        interface: Network interface name
        sample_rate: Readings are taken at this rate
        save_rate: Readings collected during this interval are averaged, result is stored in data
        history_size: Data will only be available for the past history_size readings
    """
    def __init__(self, interface: str, sample_rate: int, save_rate: int, history_size: int=1000):
        if sample_rate > save_rate:
            logger.warning('Sampling rate is longer than the save rate, so entries will be duplicated.')

        self.interface = interface
        self.sample_rate = sample_rate
        self.save_rate = save_rate
        self.data = collections.deque(maxlen=history_size)

        self._samples = collections.deque(maxlen=max(1, int(save_rate / sample_rate)) + 1)

        self._last_save_time = datetime.now()
        self._last_sample_time = datetime.now()

    def start(self):
        self._samples.clear()
        self._last_save_time = datetime.now()
        self._last_sample_time = datetime.now()

        self._sample_timer.start()
        self._save_timer.start()

    def stop(self):
        pass

    def sample_callback(self):
        """Record the latest sample."""
        self._samples.append(ifstat()[self.interface]['tx_bytes'])
        self._last_sample_time = datetime.now()

    def save_callback(self):
        """
        Write average of samples to data. Handle edge case where
        _last_sample_time happens after we were supposed to save.
        """
        now = datetime.now()
        elapsed_s = (now - self._last_save_time).total_seconds()
        excess_s = elapsed_s - self.save_rate
        extra_sample = None
        if (now - self._last_sample_time).total_seconds() < excess_s:
            # we sampled after we were supposed to save
            extra_sample = self._samples.pop()

        first = self._samples.popleft()
        last = self._samples.pop()
        self.data.append((now, last - first))

        self._samples.clear()
        if extra_sample is not None:
            self._samples.append(extra_sample)

    @property
    def data(self):
        return self.data

    def reset(self):
        pass

