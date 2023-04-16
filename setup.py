# zodbtools | pythonic package setup
from setuptools import setup, find_packages

# read file content
def readfile(path):
    with open(path, 'r') as f:
        return f.read()

setup(
    name        = 'zodbtools',
    version     = '0.0.0.dev9',
    description = 'ZODB-related utilities',
    long_description = '%s\n----\n\n%s' % (
                            readfile('README.rst'), readfile('CHANGELOG.rst')),
    url         = 'https://lab.nexedi.com/nexedi/zodbtools',
    license     = 'GPLv3+ with wide exception for Open-Source; ZPL 2.1',
    author      = 'Nexedi + Zope Foundation + Community',
    author_email= 'kirr@nexedi.com',

    keywords    = 'zodb utility tool',

    packages    = find_packages(),
    install_requires = ['ZODB', 'zodburi', 'zope.interface', 'pygolang >= 0.0.0.dev6', 'six', 'dateparser'],

    extras_require = {
                  'test': ['pytest', 'freezegun', 'pytz', 'mock;python_version<="2.7"'],
    },

    entry_points= {'console_scripts': ['zodb = zodbtools.zodb:main']},

    # FIXME restore py3 support
    classifiers = [_.strip() for _ in """\
        Development Status :: 3 - Alpha
        Intended Audience :: Developers
        Operating System :: POSIX :: Linux
        Programming Language :: Python :: 2
        Programming Language :: Python :: 2.7
        Topic :: Database
        Topic :: Utilities
        Framework :: ZODB\
    """.splitlines()]
)
