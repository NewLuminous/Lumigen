from __future__ import annotations
import typing as t
import subprocess


def submit_job(command: t.Union[str, t.List[str]]) -> t.Optional[int]:
    """Launches a command as a subprocess and streams its output."""
    p = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for line in p.stdout:
            print(line, end='')
    finally:
        if p.stdout and hasattr(p.stdout, "close"):
            p.stdout.close()
        
    p.wait()
    return p.returncode