# -*- coding: utf-8 -*-
# Copyright (C) 2024-  Nexedi SA and Contributors.
#                          Jérome Perrin <jerome@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
"""Zodbshow - Print an object stored in ZODB database from its oid"""

from __future__ import print_function
import codecs
import collections
import io
import pprint
import sys

from golang import func, defer, b
import six
import ZODB.utils
from ZODB._compat import PersistentUnpickler
from zodbtools.util import storageFromURL



PersistentReference = collections.namedtuple(
    'PersistentReference', ['p_oid', 'class_meta'])


class Instance(object):
    _mode = 'state'
    def __init__(self, *state):
        self._state = state
        if state:
            self._mode = 'reduce'
    def __setstate__(self, state):
        self._state = state
    def __repr__(self):
        return 'Instance(class=%s, %s=%s)' % (
            self.__class__,
            self._mode,
            pprint.pformat(self._state)
        )


def load_persistent(pid):
    try:
        p_oid, class_meta = pid
        p_oid = str(ZODB.utils.oid_repr(b(p_oid)))
    except ValueError:
        p_oid = pid
        class_meta = '?'
    return PersistentReference(p_oid, class_meta)


def find_global(module, name):
    return type(name, (Instance,), {'__module__': module})


def zodbshow(storage, oid):
    try:
        # 0x123456 from oid_repr
        oid = ZODB.utils.repr_to_oid(oid)
    except (ValueError, TypeError):
        # '\x00\x00\x00\x00\x00\x124V' from dump --pretty zpickledis
        oid = codecs.decode(oid, 'unicode_escape').encode()

    data = storage.load(oid)[0]
    unpickler = PersistentUnpickler(find_global, load_persistent, io.BytesIO(data))

    pprint.pprint(unpickler.load())
    pprint.pprint(unpickler.load())


# ----------------------------------------
import getopt

summary = "print an object stored in a ZODB database"

def usage(out):
    print("""\
Usage: zodb info [OPTIONS] <storage> oid
Print an object stored in a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

Options:

    -h  --help      show this help
""", file=out)


@func
def main(argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, arg in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    try:
        storurl, oid = argv
    except ValueError:
        usage(sys.stderr)
        sys.exit(2)

    stor = storageFromURL(storurl, read_only=True)
    defer(stor.close)

    zodbshow(stor, oid)
