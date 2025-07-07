from __future__ import annotations
import typing as t

import re

def extract_params(string: str) -> t.List[str]:
    match = re.search(r'([<{(\[])([^>}\])]*)([}>)\]])', string)
    if match:
        content = match.group(2)
        if content:
            return [item.strip() for item in content.split(',')]
    return []

def extract_name_wo_params(string: str) -> t.List[str]:
    match = re.match(r'^(.*?)([<{(\[])', string)
    if match:
        return match.group(1).strip()
    return string.strip()