def coalesce(*arg):
    for el in arg:
        if el is not None:
            return el
    return None