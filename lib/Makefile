 
TOP_DIR := .
SRC_DIR := $(TOP_DIR)/honeycomb
BUILD_DIR := $(TOP_DIR)/build
DIST_DIR := $(TOP_DIR)/dist
TEST_DIR := $(TOP_DIR)/honeycomb/testing
VERSION_FILE := $(SRC_DIR)/__init__.py
REQUIREMENTS_FILE := $(TOP_DIR)/requirements.txt

LIB_VERSION := 0.0.1
#LIB_VERSION := $(shell sed -ne 's/[^0-9]*\(\([0-9]\.\)\{0,4\}[0-9][^.]\).*/\1/p' $(VERSION_FILE))
#LIB_VERSION := $(shell git describe --tags --abbrev=0 | sed -Ee 's/^v|-.*//')

LIB := honeycomb-$(LIB_VERSION)-py3-none-any.whl
TARGET := $(DIST_DIR)/$(LIB)

FIND := $(shell which find)
SRCS := $(shell $(FIND) $(SRC_DIR) -type f -name "*.py")

ifeq ($(OS),Windows_NT)
    PYTHON := py.exe
else
    PYTHON := python3
endif

PYTEST := $(PYTHON) -m pytest --cov=honeycomb
YAPF := $(PYTHON) -m yapf
PIP_INSTALL := $(PYTHON) -m pip install
PIP_UNINSTALL := $(PYTHON) -m pip uninstall -y
COMPILE.py := $(PYTHON) $(TOP_DIR)/setup.py sdist bdist_wheel
RM := rm -rf
CD := cd


.PHONY: all clean distclean dist install uninstall test format depends help


all: dist

install: depends dist 
	$(PIP_INSTALL) --force-reinstall $(TARGET)

dist: dependency-check $(TARGET)

$(TARGET): $(SRCS)
	$(YAPF) -i --style=google $^
	$(COMPILE.py) 

depends: check-pip
	$(PIP_INSTALL) -r $(REQUIREMENTS_FILE)

format: dependency-check
	$(YAPF) -i --style=google $(SRCS)

clean: 
	find $(SRC_DIR) -name \*.pyc -exec rm -f {} \;
	find $(SRC_DIR) -name \*.pyo -exec rm -f {} \;

distclean: clean
	$(RM) $(BUILD_DIR)
	$(RM) $(DIST_DIR)
	$(RM) *.egg-info

test: depends
	$(CD) $(TEST_DIR) && $(PYTEST) 

uninstall:
	$(PIP_UNINSTALL) $(TARGET)

help:
	$(info TOP_DIR: $(TOP_DIR))
	$(info SRC_DIR: $(SRC_DIR))
	$(info BUILD_DIR: $(BUILD_DIR))
	$(info DIST_DIR: $(DIST_DIR))
	$(info TEST_DIR: $(TEST_DIR))
	$(info LIB_VERSION: $(LIB_VERSION))
	$(info TARGET: $(TARGET))
	$(info )
	$(info $$> make [all|dist|install|uninstall|clean|distclean|format|depends|test])
	$(info )
	$(info       all          - build library: [$(LIB)]. This is the default)
	$(info       dist         - build library: [$(LIB)])
	$(info       install      - installs: [$(LIB)])
	$(info       uninstall    - uninstalls: [$(LIB)])
	$(info       clean        - removes build artifacts)
	$(info       distclean    - removes library)
	$(info       format       - format source code)
	$(info       depends      - installs library dependencies)
	$(info       test         - run unit tests)
	@true

YAPF_CMD := $(PYTHON) -c 'import yapf' > /dev/null 2>&1
HAS_YAPF := $(shell $(YAPF_CMD) > /dev/null && echo true || echo false)
SETUP_TOOLS_CMD := $(PYTHON) -c 'import setuptools' > /dev/null 2>&1
HAS_SETUP_TOOLS := $(shell $(SETUP_TOOLS_CMD) > /dev/null && echo true || echo false)
WHEEL_CMD := $(PYTHON) -c 'import wheel' > /dev/null 2>&1 
HAS_WHEEL := $(shell $(WHEEL_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_SETUP_TOOLS),true)
ifeq ($(HAS_WHEEL),true)
ifeq ($(HAS_YAPF),true)
	SYSTEM_OK = true
endif
endif
endif

dependency-check: check-pip check-yapf check-setup-tools check-wheel 

check-pip:
	$(PYTHON) -m pip install --upgrade pip 

check-yapf: 
ifneq ($(HAS_YAPF),true)
	$(PIP_INSTALL) yapf
endif

check-setup-tools: 
ifneq ($(HAS_SETUP_TOOLS),true)
	$(PIP_INSTALL) setuptools
endif

check-wheel: 
ifneq ($(HAS_WHEEL),true)
	$(PIP_INSTALL) wheel
endif

