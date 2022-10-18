import importlib
import inspect
import pkgutil
import logging
import random
import sys
import time

from typing import List, Dict

import functools
from functools import partial

logging_logger = logging.getLogger(__name__)

try:
    from decorator import decorator
except ImportError:

    def decorator(caller):
        """ Turns caller into a decorator.
        Unlike decorator module, function signature is not preserved.

        :param caller: caller(f, *args, **kwargs)
        """

        def decor(f):

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                return caller(f, *args, **kwargs)

            return wrapper

        return decor


def _retry(f,
           exceptions=Exception,
           tries=-1,
           delay=0,
           max_delay=None,
           backoff=1,
           jitter=0,
           logger=logging_logger):
    """
    Executes a function and retries it if it failed.

    :param f: the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    _tries, _delay = tries, delay
    while _tries:
        try:
            return f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                logger.warning('%s, retrying in %s seconds...', e, _delay)

            time.sleep(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry(exceptions=Exception,
          tries=-1,
          delay=0,
          max_delay=None,
          backoff=1,
          jitter=0,
          logger=logging_logger):
    """Returns a retry decorator.

    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: a retry decorator.
    """

    @decorator
    def retry_decorator(f, *fargs, **fkwargs):
        args = fargs if fargs else list()
        kwargs = fkwargs if fkwargs else dict()
        return _retry(partial(f, *args, **kwargs), exceptions, tries, delay,
                      max_delay, backoff, jitter, logger)

    return retry_decorator


def retry_call(f,
               fargs=None,
               fkwargs=None,
               exceptions=Exception,
               tries=-1,
               delay=0,
               max_delay=None,
               backoff=1,
               jitter=0,
               logger=logging_logger):
    """
    Calls a function and re-executes it if it failed.

    :param f: the function to execute.
    :param fargs: the positional arguments of the function to execute.
    :param fkwargs: the named arguments of the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    args = fargs if fargs else list()
    kwargs = fkwargs if fkwargs else dict()
    return _retry(partial(f, *args, **kwargs), exceptions, tries, delay,
                  max_delay, backoff, jitter, logger)


def dbutils(spark):
    dbutils = None
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    else:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


class Singleton:

    def __init__(self, cls):
        self._cls = cls

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._cls()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)


def find_modules(package, recursive=True):
    """
    Finds all submodules given a package

    :param package: The base package to search from
    :param recursive: Perform a recursive search
    :returns: A list of modules
    """

    if isinstance(package, str):
        try:
            package = importlib.import_module(package)
        except ImportError:
            return []

    submodules = []
    for _loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + "." + name

        try:
            submodules.append(importlib.import_module(full_name))
        except ImportError:
            continue

        if recursive and is_pkg:
            submodules += pkg_submodules(full_name)

    return submodules


def is_child_of(parent) -> bool:
    """
    :param parent: The parent
    :returns: A lambda filter
    """
    return lambda member: inspect.isclass(member) and issubclass(
        member, parent) and member != parent


def find_members(module_name, predicate):
    """
    Finds all members (ie classes) in a given module

    :param module_name: The base package to search from
    :param predicate: A filter predicate
    :returns: A list of modules
    """
    modules = find_modules(module_name)
    members = []
    for module in modules:
        for name, member in inspect.getmembers(module, predicate):
            members.append(member)

    return members
