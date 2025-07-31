from __future__ import annotations
import typing as t
import subprocess


def submit_job(command: t.Union[str, t.List[str]]) -> int | None:
    """Launches a command as a subprocess and streams its output."""
    p = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, stderr=subprocess.STDOUT)
    while True:
        output = p.stdout.readline()
        if output == '' and p.poll() is not None:
            break
        if output:
            print(output.rstrip())
    rc = p.poll()
    return rc