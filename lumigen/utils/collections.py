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


SQL_IDENTIFIER_QUOTES = {
    '"': '"',
    '`': '`',
    '[': ']'
}

def _strip_quotes(item: str) -> t.Tuple[bool, str]:
    """
    Checks if a SQL identifier is wrapped in matching quote characters.
    Returns a tuple of (is_quoted, inner_unquoted_text).
    """
    if len(item) >= 2:
        first_char = item[0]
        last_char = item[-1]
        if first_char in SQL_IDENTIFIER_QUOTES and SQL_IDENTIFIER_QUOTES[first_char] == last_char:
            return True, item[1:-1]
    return False, item

class SQLIdentifierSet(MutableSet):
    """
    A set-like container designed specifically for matching SQL identifiers.
    
    It simplifies membership testing by matching across standard quote characters
    (e.g., `col`, "col", [col]) and falls back to case-insensitive checks.
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
        is_quoted, inner_text = _strip_quotes(item)
        if is_quoted:
            # Rule Set A: Item starts and ends with a quote char
            # 1. Exact match
            if item in self._data:
                return item
            
            # 2. Different quote chars but exact inner text
            for start_q, end_q in SQL_IDENTIFIER_QUOTES.items():
                alt_quoted = f"{start_q}{inner_text}{end_q}"
                if alt_quoted in self._data:
                    return alt_quoted
            
            # 3. Matches exactly inner_text
            if inner_text in self._data:
                return inner_text
            
            return None
        else:
            # Rule Set B: Item does not start and end with a quote char
            # 1. Exact match
            if item in self._data:
                return item
            
            # 2. Has quote characters around but [1:-1] is exactly the same
            for start_q, end_q in SQL_IDENTIFIER_QUOTES.items():
                alt_quoted = f"{start_q}{item}{end_q}"
                if alt_quoted in self._data:
                    return alt_quoted
            
            # 3. All-uppercase match
            upper_item = item.upper()
            if upper_item in self._data:
                return upper_item
            
            # 4. Case-insensitive match (first added variant)
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
        self._data.add(item_str)
        
        lower_item = item_str.lower()
        if lower_item not in self._map:
            self._map[lower_item] = item_str

    def discard(self, item: str):
        """Removes an item from the set if it is present."""
        item_to_remove = self._find_actual_item(str(item))
        
        if item_to_remove:
            self._data.discard(item_to_remove)
            
            lower_item = item_to_remove.lower()
            # If the removed item was the canonical map reference, rebuild it if necessary
            if self._map.get(lower_item) == item_to_remove:
                del self._map[lower_item]
                
                for k in self._data:
                    if k.lower() == lower_item:
                        self._map[lower_item] = k
                        break

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)
    
    def __repr__(self) -> str:
        if not self._data:
            return "SQLIdentifierSet()"
        return f"SQLIdentifierSet({list(self._data)})"

class SQLIdentifierDict(UserDict):
    """
    A dictionary-like container designed specifically for matching SQL identifiers.
    
    Useful for looking up column types or configuration options where the incoming 
    identifier may vary in casing or quoting style compared to the stored key.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._map: t.Dict[str, str] = {}
        # Populate map with the initial data keys
        for k in self.data:
            lower_k = str(k).lower()
            if lower_k not in self._map:
                self._map[lower_k] = str(k)

    def _find_actual_key(self, key: str) -> t.Optional[str]:
        """
        Internal helper to find the stored dictionary key using fallback logic.
        """
        is_quoted, inner_text = _strip_quotes(key)

        if is_quoted:
            # Rule Set A: Key starts and ends with a quote char
            if key in self.data:
                return key
            for start_q, end_q in SQL_IDENTIFIER_QUOTES.items():
                alt_quoted = f"{start_q}{inner_text}{end_q}"
                if alt_quoted in self.data:
                    return alt_quoted
            if inner_text in self.data:
                return inner_text
            return None
        else:
            # Rule Set B: Key does not start and end with a quote char
            if key in self.data:
                return key
            for start_q, end_q in SQL_IDENTIFIER_QUOTES.items():
                alt_quoted = f"{start_q}{key}{end_q}"
                if alt_quoted in self.data:
                    return alt_quoted
            upper_key = key.upper()
            if upper_key in self.data:
                return upper_key
            lower_key = key.lower()
            if lower_key in self._map:
                return self._map[lower_key]
            return None

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
        actual_key = self._find_actual_key(key_str)
        
        if actual_key is not None:
            return self.data[actual_key]

        # Fallback: return the queried key itself
        return key_str

    def __delitem__(self, key: str):
        """Deletes a key using the fallback strategy."""
        key_str = str(key)
        actual_key = self._find_actual_key(key_str)
        
        if actual_key is not None:
            super().__delitem__(actual_key)
            
            lower_actual = actual_key.lower()
            # If the deleted item was the canonical map reference, remove/rebuild
            if self._map.get(lower_actual) == actual_key:
                del self._map[lower_actual]
                
                for k in self.data:
                    if str(k).lower() == lower_actual:
                        self._map[lower_actual] = str(k)
                        break
        else:
            # Fallback to default behavior which will naturally raise a KeyError
            super().__delitem__(key_str)

    def __contains__(self, key: object) -> bool:
        """Checks for key existence using the full fallback logic."""
        return self._find_actual_key(str(key)) is not None