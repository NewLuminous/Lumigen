import datetime


def format_seconds_timestamp(sec_timestamp: int) -> str:
    """
    Converts a Unix timestamp (in seconds) into an ISO 8601 formatted string
    with zero-padded nanosecond precision.

    The format is YYYY-MM-DDTHH:MM:SS.000000000Z, representing UTC time.

    Args:
        sec_timestamp: An integer representing the total seconds
                       since the Unix epoch (1970-01-01 00:00:00 UTC).

    Returns:
        A string representing the timestamp in the specified format.
        
    Example:
        >>> format_seconds_timestamp(1749124481)
        '2025-06-05T11:54:41.000000000Z'
    """
    dt_object = datetime.datetime.utcfromtimestamp(sec_timestamp)
    main_part = dt_object.strftime('%Y-%m-%dT%H:%M:%S')
    return f"{main_part}.000000000Z"

def format_nanosecond_timestamp(ns_timestamp: int) -> str:
    """
    Converts a nanosecond Unix timestamp into an ISO 8601 formatted string.

    The format is YYYY-MM-DDTHH:MM:SS.fffffffffZ, representing UTC time.

    Args:
        ns_timestamp: An integer representing the total nanoseconds
                      since the Unix epoch (1970-01-01 00:00:00 UTC).

    Returns:
        A string representing the timestamp in ISO 8601 format with
        nanosecond precision and a 'Z' for UTC.
        
    Example:
        >>> format_nanosecond_timestamp(1749124481528122919)
        '2025-06-05T11:54:41.528122919Z'
    """
    seconds = ns_timestamp // 1_000_000_000
    nanoseconds = ns_timestamp % 1_000_000_000

    # Use utcfromtimestamp to make it clear we are working in UTC.
    dt_object = datetime.datetime.utcfromtimestamp(seconds)
    main_part = dt_object.strftime('%Y-%m-%dT%H:%M:%S')
    nano_part = f".{nanoseconds:09d}"
    return f"{main_part}{nano_part}Z"