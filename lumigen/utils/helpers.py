import os
import re
from pathlib import Path

def coalesce(*arg):
    for el in arg:
        if el is not None:
            return el
    return None

def get_system_timezone() -> str:
    # Use TZ if already set in environment
    if tz := os.getenv("TZ"):
        return tz

    # Check /etc/timezone (Debian / Ubuntu)
    tz_file = Path("/etc/timezone")
    if tz_file.is_file():
        try:
            content = tz_file.read_text().strip()
            if content:
                return content
        except Exception:
            pass

    # Check /etc/localtime symlink (RHEL / CentOS / Arch / systemd)
    localtime = Path("/etc/localtime")
    if localtime.is_symlink():
        try:
            target = os.readlink(localtime)
            if "zoneinfo/" in target:
                return target.split("zoneinfo/")[-1]
        except Exception:
            pass

    # Check /etc/sysconfig/clock (Older RHEL / CentOS)
    clock_file = Path("/etc/sysconfig/clock")
    if clock_file.is_file():
        try:
            match = re.search(r'ZONE="?([^"\n]+)"?', clock_file.read_text())
            if match:
                return match.group(1)
        except Exception:
            pass

    # Use tzlocal (Cross-platform / Windows / macOS / Binary tzfiles)
    try:
        import tzlocal
        if tz_name := tzlocal.get_localzone_name():
            return tz_name
    except Exception:
        pass

    return "UTC"