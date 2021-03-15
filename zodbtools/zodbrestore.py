# Copyright (C) 2021  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
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
"""Zodbrestore - Restore content of a ZODB database.

Zodbrestore reads transactions from zodbdump output and recreates them in a
ZODB storage. See Zodbdump documentation for details.
"""

from __future__ import print_function
from zodbtools.zodbdump import DumpReader
from zodbtools.zodbcommit import zodbcommit, _low_level_note
from zodbtools.util import asbinstream, ashex, storageFromURL
from golang import func, defer


# zodbrestore restores transactions read from reader r in zodbdump format.
#
# restoredf, if !None, is called for every restored transaction.
def zodbrestore(stor, r, restoredf=None):
    zr = DumpReader(r)
    at = stor.lastTransaction()
    while 1:
        txn = zr.readtxn()
        if txn is None:
            break

        zodbcommit(stor, at, txn)
        if restoredf != None:
            restoredf(txn)
        at = txn.tid


# ----------------------------------------
import sys, getopt

summary = "restore content of a ZODB database"

def usage(out):
    print("""\
Usage: zodb restore [OPTIONS] <storage> < input
Restore content of a ZODB database.

The transactions to restore are read from stdin in zodbdump format.
On success the ID of every restored transaction is printed to stdout.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

Options:

    -h  --help      show this help
""" + (_low_level_note % "zodb restore"), file=out)

@func
def main(argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    if len(argv) != 1:
        usage(sys.stderr)
        sys.exit(2)

    storurl = argv[0]

    stor = storageFromURL(storurl)
    defer(stor.close)

    def _(txn):
        print(ashex(txn.tid))
    zodbrestore(stor, asbinstream(sys.stdin), _)
