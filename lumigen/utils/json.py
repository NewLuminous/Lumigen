import typing as t
import json
import base64
import datetime
import numpy as np


class PortableJSONEncoder(json.JSONEncoder):
    """
    A JSON encoder that can handle additional data types by converting them
    to a standard, portable JSON representation.
    """
    def default(self, obj):
        if isinstance(obj, (bytes, bytearray)):
            return base64.b64encode(obj).decode('utf-8')
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()

        # Let the base class handle the rest, or raise a TypeError
        return super().default(obj)

class DisplayableJSONEncoder(PortableJSONEncoder):
    """
    Extends PortableJSONEncoder to provide more human-readable output for
    certain types, primarily for display or debugging purposes.
    """
    def default(self, obj):
        if isinstance(obj, (bytearray, bytes)):
            try:
                # Try to decode using a forgiving encoding for a prettier output.
                return "b'" + obj.decode('latin-1') + "'"
            except UnicodeDecodeError:
                return super().default(obj)
        
        return super().default(obj)
    
def _get_value_type_name(value: t.Any) -> str:
    if value is None: return 'NoneType'
    if isinstance(value, (bytes, bytearray)): return 'bytes'
    if isinstance(value, datetime.datetime): return 'datetime'
    if isinstance(value, datetime.date): return 'date'
    if isinstance(value, datetime.time): return 'time'
    if isinstance(value, np.integer): return 'int'
    if isinstance(value, np.floating): return 'float'
    if isinstance(value, np.ndarray): return 'numpy.ndarray'
    return value.__class__.__name__
    
def encode_typed_value(value: t.Any) -> t.Tuple[str, str]:
    """
    Encodes a Python object into a (value_string, type_string) tuple.
    The value_string is a human-readable JSON representation.

    Returns:
        A tuple of (json_string, type_name).
    
    Raises:
        TypeError: If the object's type is not serializable by PortableJSONEncoder.
    """
    value_string = json.dumps(value, cls=PortableJSONEncoder)
    type_string = _get_value_type_name(value)
    
    return value_string, type_string

def decode_typed_value(value_str: str, type_str: str) -> t.Any:
    """
    Decodes a value from its string representation and type string back into a Python object.
    
    Note: For lists and dictionaries, this function doesn't recursively decode
    nested special types. The consumer of the data is expected to handle that if needed.
    This function primarily reconstructs the top-level object.
    """
    if type_str == 'NoneType': return None
    if value_str is not None and not isinstance(value_str, (str, bytes, bytearray)):
        value_str = str(value_str)

    value = json.loads(value_str)

    if type_str in ['str', 'bool', 'int', 'float', 'list', 'dict']: return value

    if type_str == 'bytes': return base64.b64decode(value)
    if type_str == 'datetime': return datetime.datetime.fromisoformat(value)
    if type_str == 'date': return datetime.date.fromisoformat(value)
    if type_str == 'time': return datetime.time.fromisoformat(value)
    if type_str == 'numpy.ndarray': return np.array(value)

    return value