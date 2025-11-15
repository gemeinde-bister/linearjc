#!/usr/bin/env python3
"""
LinearJC Coordinator - Setup configuration
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read requirements
requirements = []
with open("requirements.txt") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            requirements.append(line)

# Read README for long description
readme_file = Path("README.md")
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="linearjc-coordinator",
    version="0.1.0",
    description="LinearJC Coordinator - Distributed job scheduling system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="LinearJC",
    license="MIT",
    python_requires=">=3.9",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "linearjc-coordinator=coordinator.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
