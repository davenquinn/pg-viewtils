from os import path

def relative_path(base, *parts):
    """
    Relative path, given a base file or directory
    """
    if not path.isdir(base):
        base = path.dirname(base)
    return path.abspath(path.join(base, *parts))
