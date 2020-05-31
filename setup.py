#!/usr/bin/env python3
from pathlib import Path
from setuptools import setup, find_packages


readme = Path("README.md").read_text()
requirements = Path("requirements.txt").read_text().split("\n")

setup(
    name="elastic_datashader",
    author="Spectric Labs",
    author_email="foss@spectric.com",
    description="Elastic-Datashader TMS Server",
    url="https://github.com/spectriclabs/elastic_datashader",
    version="0.0.4",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
    extras_require={"apm": ["elasticapm"]},
    entry_points={"console_scripts": ["tms_datashader=tms_datashader:main"]},
)
