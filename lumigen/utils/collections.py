import copy
import typing as t
from collections import UserDict
from collections.abc import MutableSet
from functools import reduce


def merge_dicts(*args: t.Dict) -> t.Dict:
    """
    Recursively merges dictionaries.
    The last dictionary in the arguments list has the highest precedence.
    """
    def merge(a: t.Dict, b: t.Dict) -> t.Dict:
        for b_key, b_value in b.items():
            a_value = a.get(b_key)
            if isinstance(a_value, dict) and isinstance(b_value, dict):
                merge(a_value, b_value)
            elif isinstance(b_value, dict):
                a[b_key] = copy.deepcopy(b_value)
            else:
                a[b_key] = b_value
        return a

    return reduce(merge, args, {})

def rename_key(data, key_change_dict):
    keys = list(data.keys())
    for key in keys:
        if key in key_change_dict:
            data[key_change_dict[key]] = data.pop(key)
    return data

class FallbackSet(MutableSet):
    """
    A set-like object with a prioritized key lookup strategy.

    When checking for membership or getting an item, it follows this order:
    1. Exact, case-sensitive match.
    2. All-uppercase match.
    3. Case-insensitive match (based on the first version of the item added).
    """
    def __init__(self, initial: t.Iterable[str] = None):
        self._data: t.Set[str] = set()
        self._map: t.Dict[str, str] = {}
        
        if initial is not None:
            for item in initial:
                self.add(item)

    def _find_actual_item(self, item: str) -> t.Optional[str]:
        """
        Internal helper to find the stored item using the fallback logic.
        Returns the actual stored string or None if not found.
        """
        # 1. Exact match
        if item in self._data:
            return item
        
        # 2. All-uppercase match
        upper_item = item.upper()
        if upper_item in self._data:
            return upper_item
            
        # 3. Case-insensitive match
        lower_item = item.lower()
        if lower_item in self._map:
            return self._map[lower_item]
        
        return None

    def get(self, item: str, default: t.Optional[str] = None) -> t.Optional[str]:
        found_item = self._find_actual_item(str(item))
        return found_item if found_item is not None else default

    def __contains__(self, item: object) -> bool:
        """Checks for item existence using the `in` operator."""
        return self._find_actual_item(str(item)) is not None

    def add(self, item: str):
        item_str = str(item)
        lower_item = item_str.lower()
        
        if lower_item not in self._map:
            self._data.add(item_str)
            self._map[lower_item] = item_str

    def discard(self, item: str):
        """Removes an item from the set if it is present."""
        item_to_remove = self._find_actual_item(str(item))
        
        if item_to_remove:
            self._data.discard(item_to_remove)
            del self._map[item_to_remove.lower()]

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)
    
    def __repr__(self) -> str:
        if not self._data:
            return "FallbackSet()"
        return f"FallbackSet({list(self._data)})"

class FallbackKeyDict(UserDict):
    """
    A dictionary-like object with a prioritized key lookup strategy.

    When getting an item, it attempts to find a key in the following order:
    1. Exact, case-sensitive match.
    2. All-uppercase match.
    3. Case-insensitive match (the first one added if multiple exist).
    4. If no match is found, returns the queried key itself.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # _map stores a lowercase key to the original cased key
        self._map: t.Dict[str, str] = {str(k).lower(): str(k) for k in self.data}

    def __setitem__(self, key: str, value: t.Any):
        """Stores the key-value pair and updates the case-insensitive map."""
        key_str = str(key)
        super().__setitem__(key_str, value)
        
        # Only add to the lowercase map if it's not already there.
        # This gives precedence to the first key of a given case-insensitive form.
        lower_key = key_str.lower()
        if lower_key not in self._map:
            self._map[lower_key] = key_str

    def __getitem__(self, key: str) -> t.Any:
        key_str = str(key)

        # 1. Exact match (fastest)
        if key_str in self.data:
            return self.data[key_str]

        # 2. All-uppercase match
        upper_key = key_str.upper()
        if upper_key in self.data:
            return self.data[upper_key]

        # 3. Case-insensitive match using the map
        lower_key = key_str.lower()
        if lower_key in self._map:
            original_key = self._map[lower_key]
            return self.data[original_key]

        # 4. Fallback: return the queried key itself
        return key_str

    def __delitem__(self, key: str):
        """Deletes a key using a case-insensitive search."""
        key_str = str(key)
        lower_key = key_str.lower()
        
        if lower_key in self._map:
            original_key = self._map.pop(lower_key)
            super().__delitem__(original_key)
        else:
            # Fallback to default behavior which will raise a KeyError
            super().__delitem__(key_str)

    def __contains__(self, key: object) -> bool:
        """Checks for key existence using the full fallback logic."""
        key_str = str(key)
        if key_str in self.data:
            return True
        if key_str.upper() in self.data:
            return True
        if key_str.lower() in self._map:
            return True
        return False