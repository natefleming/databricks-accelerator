import logging

from typing import List, Dict
import distutils.util

logging_logger = logging.getLogger(__name__)


def lower(value: str) -> str:
    return value.lower()


def strtobool(value: str) -> bool:
    return bool(distutils.util.strtobool(value)) if value else False


def split(value: str, delimiter=',', num: int = -1, action=None) -> List[str]:
    if value is None:
        return None
    action = action if action else lambda x: x
    results = [
        action(x.strip()) for x in value.split(delimiter, num) if x.strip()
    ]
    padding = [None] * max(0, num - len(results))
    return [*results, *padding]


def split_dict(value: str, action=None) -> Dict[str, str]:
    if value is None:
        return None
    results = dict(
        split(x, delimiter='=', num=1, action=action)
        for x in split(value)) if value else {}
    return results


def get_value(value: str, default=None, action=None) -> str:
    if value is None:
        return default
    action = action if action else lambda x: x
    value = value.strip()
    return action(value) if value else default
