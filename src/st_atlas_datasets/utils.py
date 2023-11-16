import re


def to_snake_case(s: str) -> str:
    if not isinstance(s, str):
        raise TypeError(f"Expected str, got {type(s)}")
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("__([A-Z])", r"_\1", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower().replace("-", "_")
