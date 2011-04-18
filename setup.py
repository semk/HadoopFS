#!/usr/bin/env python

import distutils.core
import sys

# Importing setuptools adds some features like "setup.py develop", but
# it's optional so swallow the error if it's not there.
try:
    import setuptools
except ImportError:
    pass

version = "1.0"

distutils.core.setup(
    name="hadoopfs",
    version=version,
    packages = ["hadoopfs"],
    author="Sreejith K",
    author_email="sreejithemk@gmail.com",
    url="http://www.foobarnbaz.org/",
    download_url="http://github.com/semk/HadoopFS",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="Thrift based client library for Hadoop Distributed FileSytem (HDFS)",
)
