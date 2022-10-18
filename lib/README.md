# Honeycomb Databricks Library

## Prequisites
 - python3
 - bash
 - make

## Set up virtual environment (Recommended)

It is recommended that you create a virtual python environment to manage dependencies.
```
$> python3 -m venv venv
```
Activate this environment.
```
$> source ./venv/bin/activate
```
After activating your virtual environment, running:
```
$> which python3
```
should display the python3 binary relative to your virtual environment. (ie ./venv/bin/python3)

## Building

This will format the source code as well as create the the Honeycomb library.
The generated library can be installed directly on the databricks cluster

```
$> make 
```
OR
```
$> make dist
```
```
honeycomb-engineering/databricks/lib/dist/honeycomb-x.x.x-py3-none-any.whl
```

## Installing

Install the Honeycomb library locally along with depdendencies.
``` 
$> make install
```

## Updating Honeycomb Databricks Library Version  

The following script has been provided to increment the embedded version number for honeycomb. This script only
updates the minor patch version [major.minor.patch]. Updating the major or minor version must be done manually by 
updating the `__version__` variable in `honeycomb/__init__.py` 

```
$> honeycomb-engineering/databricks/lib/scripts/update_version.py
```

## Python Source Code Formatting  

A script has been provided which formats python source code. This should be run to ensure coding standards.
```
$> make format
```

## Unit Tests
```
$> make test
```
OR
```
$> cd honeycomb-engineering/databricks/lib/honeycomb/testing
$> pytest [filename]
```

## Display Help
```
(venv) INMAC-WKS999:lib nate.fleming$ make help
TOP_DIR: .
SRC_DIR: ./honeycomb
BUILD_DIR: ./build
DIST_DIR: ./dist
TEST_DIR: ./honeycomb/testing
LIB_VERSION: 0.0.1
TARGET: ./dist/honeycomb-0.0.1-py3-none-any.whl

$> make [all|dist|install|uninstall|clean|distclean|format|depends|test]

all          - build library: [honeycomb-0.0.1-py3-none-any.whl]. This is the default
dist         - build library: [honeycomb-0.0.1-py3-none-any.whl]
install      - installs: [honeycomb-0.0.1-py3-none-any.whl]
uninstall    - uninstalls: [honeycomb-0.0.1-py3-none-any.whl]
clean        - removes build artifacts
distclean    - removes library
format       - format source code
depends      - installs library dependencies
test         - run unit tests

```


