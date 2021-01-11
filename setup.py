from setuptools import setup
from pathlib import Path

with Path('README.md').open('r') as f:
    long_description = f.read()

with Path('requirements.txt').open('r') as f:
    requires = f.read().splitlines()

setup(
    name='primeight',
    version='0.1.4',
    url='https://pri.meight.com',
    author='Meight',
    author_email='engineering@meight.com',
    description="An Open Source Python package to manage and interact with Cassandra",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=['primeight', 'primeight.parser'],
    install_requires=requires,
    license='Apache License 2.0',
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ]
)
