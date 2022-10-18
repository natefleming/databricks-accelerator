import setuptools
import sys

with open("README.md", "r") as fin:
    long_description = fin.read()


with open("honeycomb/__init__.py", "r") as fin:
    for line in fin.readlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            version = line.split(delim)[1]

# Use default version
version='0.0.1'

setuptools.setup(
    name="honeycomb", 
    version=version,
    author="Moser Consulting",
    author_email="honeycomb@moserit.com",
    description="Common utilities for spark development in databricks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = "www.moserit.com",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
#        'decorator>=4.4.2',
#        'pandas>=1.2.0',
#        'pyspark>=3.0.1',
#        'pytest>=6.2.1',
#        'pytest-cov>=2.10.1',
#        'delta>=0.4.2',
#        'ipython>=7.24.0',
#        'pyodbc>=4.0.30',
#        'snowflake-sqlalchemy>=1.3.3',
    ],
    python_requires='>=3.7',
)
