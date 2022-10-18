#!/usr/bin/env python

import codecs
import os.path


def read(path: str):
    with codecs.open(path, 'r') as fp:
        return fp.read()


def get_version(path: str):
    for line in read(path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError('Unable to find version string.')


def increment_build_version(version: str):
    (major, minor, build) = version.split('.')
    build = str(int(build) + 1)
    return '.'.join([major, minor, build])


def replace_version(path: str, old_version: str, new_version: str):
    print(f'replacing version: {old_version} with {new_version} in file {path}')
    with open(path) as fin:
        text = fin.read().replace(old_version, new_version)

    with open(path, "w") as fout:
        fout.write(text)


def update_version(path: str):
    old_version = get_version(path)
    new_version = increment_build_version(old_version)
    replace_version(path, old_version, new_version)


version_file: str = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), 
        '..', 
        'honeycomb', 
        '__init__.py')
update_version(version_file)
