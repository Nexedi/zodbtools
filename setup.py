# zodbtools | pythonic package setup
from setuptools import setup, find_packages

# read file content
def readfile(path):
    with open(path, 'r') as f:
        return f.read()

setup(
    name        = 'zodbtools',
    version     = '0.0.0.dev1',
    description = 'ZODB-related utilities',
    long_description = readfile('README.rst'),
    url         = 'https://lab.nexedi.com/kirr/zodbtools',
    license     = 'GPLv2+',
    author      = 'Kirill Smelkov',
    author_email= 'kirr@nexedi.com',

    keywords    = 'zodb utility tool',

    package_dir = {'wendelin': ''},
    packages    = find_packages(),
    install_requires = ['ZODB'],

    classifiers = [_.strip() for _ in """\
        Development Status :: 3 - Alpha
        Intended Audience :: Developers
        Operating System :: POSIX :: Linux
        Programming Language :: Python :: 2
        Programming Language :: Python :: 2.7
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.4
        Programming Language :: Python :: 3.5
        Topic :: Database
        Topic :: Utilities
        Framework :: ZODB\
    """.splitlines()]
)
