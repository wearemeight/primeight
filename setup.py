from setuptools import setup
from pathlib import Path


requires = []
requirements_file = Path(__file__).parent.joinpath('requirements.txt')
if requirements_file.is_file():
    with requirements_file.open() as f:
        requirements = f.read()

    requires = requirements.splitlines()

setup(
    name='primeight',
    description="Primeight is packaged with all you need to create tables, "
                "query them, and insert data into your Cassandra database",
    version='0.1.1',
    url='https://pri.meight.com',
    license='Apache License 2.0',
    author='Meight team',
    author_email='engineering@meight.com',
    packages=['primeight', 'primeight.parser'],
    install_requires=requires
)
