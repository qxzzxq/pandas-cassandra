# coding: utf-8

from setuptools import setup

from pandra import __version__

setup(
    name='pandra',
    version=__version__.__version__,
    author=__version__.__author__,
    author_email=__version__.__author_email__,
    url='https://github.com/qxzzxq/pandas-cassandra',
    packages=['pandra'],
    license='MIT',
    description='Use Pandas DataFrame with Cassandra',
    keywords=['pandas', 'cassandra', 'cql'],
    install_requires=__version__.__dependencies__
)
