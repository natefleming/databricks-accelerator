#!/usr/bin/env python

import argparse
from pathlib import Path
import os
import sys
import subprocess
import tempfile
from typing import List
from datetime import datetime

DIST_DIR: str = os.path.join( os.path.abspath(os.path.dirname(__file__)), '..', 'dist')
NOTEBOOK_DIR: str = os.path.join( os.path.abspath(os.path.dirname(__file__)), '..', '..', 'notebooks', 'honeycomb')
WORKSPACE_DIR: str = '/Shared/honeycomb' 
DBFS_LIB_DIR: str = "dbfs:/mnt/lib"
EXPORT_DIR: Path = Path(tempfile.gettempdir())
LIB_EXT: str = '.whl'
PY_EXT: str = '.py'


def upgrade_libraries(profile: str) -> None:
    print('upgrade_libraries')

    for p in Path(DIST_DIR).iterdir():
        if p.is_file() and p.suffix == LIB_EXT:
            lib_path: str = p.resolve()
            cmd: str = f"databricks --profile {profile} fs cp --overwrite {lib_path} {DBFS_LIB_DIR}"
            result: subprocess.CompletedProcess = subprocess.run(cmd, shell=True)
            print(result)
            result.check_returncode()


def upgrade_notebooks(profile: str) -> None:
    print('upgrade_notebooks')

    time_now = datetime.now().strftime("%Y%m%d_%H%M%S")

    def walk(path, parent):
        for p in Path(path).iterdir():
            if p.is_dir():
                yield from walk(p, f"{parent}/{p.name}")
                continue
            file_path: str = p.resolve()
            dbfs_path: str = f"{parent}/{p.name}"
            yield (file_path, dbfs_path)

    notebook_paths = list(walk(Path(NOTEBOOK_DIR), Path(NOTEBOOK_DIR).name))

    for file_path, dbfs_path in notebook_paths:
        if file_path.is_file() and file_path.suffix == PY_EXT:
            dbfs_notebook: str = os.path.splitext(dbfs_path)[0]
            export_dir = Path(f"{EXPORT_DIR}/{profile}/{time_now}/{Path(dbfs_path).parent}")
            export_dir.mkdir(parents=True, exist_ok=True)
            cmd: str = f"databricks --profile {profile} workspace export --overwrite /Shared/{dbfs_notebook} {export_dir}"
            result: subprocess.CompletedProcess = subprocess.run(cmd, shell=True)
            print(result)

            cmd: str = f"databricks --profile {profile} workspace rm /Shared/{dbfs_notebook}"
            result: subprocess.CompletedProcess = subprocess.run(cmd, shell=True)
            print(result)

            cmd: str = f"databricks --profile {profile} workspace import {file_path} /Shared/{dbfs_notebook} --language python"
            result: subprocess.CompletedProcess = subprocess.run(cmd, shell=True)
            print(result)
            result.check_returncode()


def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Upgrade Honeycomb Databricks')
    parser.add_argument('--profile', help='The databricks-cli profile')
    parser.add_argument('--libraries', action='store_true')
    parser.add_argument('--notebooks', action='store_true')
    options = parser.parse_args(args)

    if not (options.libraries or options.notebooks):
        parser.error('Missing required option --libraries or --notebooks')

    return options
    

def main(args: List[str]) -> None:
    print(args)
    options: argparse.Namespace = parse_args(args)
    if options.libraries:
        upgrade_libraries(options.profile)

    if options.notebooks:
        upgrade_notebooks(options.profile)

if __name__ == '__main__':
    main(sys.argv[1:])
