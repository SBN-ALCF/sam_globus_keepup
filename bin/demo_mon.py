#!/usr/bin/env python

import time

from sam_globus_keepup.mon import NetworkMonitor


def main():
    nm1 = NetworkMonitor('eno1', 60, 'demo_log_eno1.txt')
    nm2 = NetworkMonitor('eno2', 60, 'demo_log_eno2.txt')
    nm1.start()
    nm2.start()
    try:
        while True:
            print("Waiting...")
            time.sleep(10)
    except KeyboardInterrupt:
        print("Ending!")
        nm1.stop()
        nm2.stop()


if __name__ == '__main__':
    main()
