from __future__ import annotations

import typing as t

from pyhocon import ConfigTree, HOCONConverter, UndefinedKey


class UnparsedKeyConfigTree(ConfigTree):
    def put(self, key, value, append=False):
        self._put([key], value, append)

    def get(self, key, default=UndefinedKey):
        return self._get([key], 0, default)


def dumps(value: t.Dict, compact: bool=False, indent: int=2, level: int=0) -> str:
    root = build_tree(value)
    hocon_str = HOCONConverter.to_hocon(root, compact=compact, indent=indent, level=level)
    
    return hocon_str


def build_tree(obj: t.Dict, depth: int=-1):
    if depth == 0:
        return UnparsedKeyConfigTree(obj)
    
    root = UnparsedKeyConfigTree()
    for k, v in obj.items():
        if isinstance(v, dict):
            root.put(k, build_tree(v, depth=depth-1))
        elif isinstance(v, list):
            all_dict_flag = all(isinstance(e, dict) for e in v)
            if all_dict_flag:
                for element in v:
                    if isinstance(element, dict):
                        root.put(k, build_tree(element, depth=depth-1), append=True)
                    else:
                        root.put(k, element, append=True)
            else:
                root.put(k, v)
        else:
            root.put(k, v)

    return root