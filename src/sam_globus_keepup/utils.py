"""
Utility functions
"""

import pathlib
import subprocess


def run_path(run_number: int):
    """Convert 8-digit int ABCXYZPQ to path AB/CX/YZ/PQ."""
    s = f'{run_number:08d}'
    return pathlib.Path(*[s[i:i+2] for i in range(0, len(s), 2)])


def du(path: pathlib.Path) -> int:
    """Return the output of du -s."""
    if not path.exists():
        raise RuntimeError(f'Path {path} does not exist.')

    return int(subprocess.check_output(['du','-sx', path]).split()[0])
