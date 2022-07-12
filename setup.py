#from setuptools import setup
from distutils.core import setup

setup(
    name='mcstools',
    version='0.1.0',
    author='Marek Slipski',
    author_email='marek.slipski@gmail.com',
    packages=['mcstools'],
    #url='http://pypi.python.org/pypi/PackageName/',
    license='LICENSE',
    description='Package to read MCS data',
    long_description=open('README.md').read(),
    install_requires=[
        "black==19.10b",
        "dask==2022.6.1",
        "distributed==2022.6.1",
        "flake8==4.0.1",
        "isort==5.10.1",
        "marstime==0.5.6",
        "numpy==1.23.0",
        "pandas==1.4.3",
        "pytest==7.1.2",
    ],
)
