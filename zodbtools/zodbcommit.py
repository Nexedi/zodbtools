# Copyright (C) 2018  Nexedi SA and Contributors.
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
"""Zodbcommit - Commit new transaction into a ZODB database

Zodbcommit reads transaction description from stdin and commits read data into
ZODB. The transaction to be committed is read in zodbdump format, but without
first 'txn' header line. For example::

    user "author"
    description "change 123"
    extension ""
    obj 0000000000000001 4 null:00
    ZZZZ

On success the ID of committed transaction is printed to stdout.
On conflict or other problem - the error is printed to stderr exit code is !0.

Zodbcommit requires `at` parameter to be given. This specifies caller idea
about its current database view and is used to detect conflicting simultaneous
commits. `at` is required because zodbcommit is plumbing-level command and
implicitly using storage last_tid instead of it could hide bugs. In scripts one
can query current database head (last_tid) with `zodb info <stor> last_tid`.
"""

from __future__ import print_function
from zodbtools import zodbdump
from zodbtools.util import ashex, storageFromURL
from ZODB.utils import p64, u64, z64
from ZODB.POSException import POSKeyError
from ZODB._compat import BytesIO
from golang import panic


# zodbcommit commits new transaction into ZODB storage with data specified by
# zodbdump transaction.
#
# txn.tid is ignored.
# tid of committed transaction is returned.
def zodbcommit(stor, at, txn):
    assert isinstance(txn, zodbdump.Transaction)

    before = p64(u64(at)+1)

    stor.tpc_begin(txn)
    for obj in txn.objv:
        data = None # data do be committed - setup vvv
        if isinstance(obj, zodbdump.ObjectCopy):
            # NEO does not support restore, and even if stor supports restore,
            # going that way requires to already know tid for transaction we are
            # committing. -> we just imitate copy by actually copying data and
            # letting the storage deduplicate it.
            data, _, _ = stor.loadBefore(obj.oid, p64(u64(obj.copy_from)+1))

        elif isinstance(obj, zodbdump.ObjectDelete):
            data = None

        elif isinstance(obj, zodbdump.ObjectData):

            if isinstance(obj.data, zodbdump.HashOnly):
                raise ValueError('cannot commit transaction with hashonly object')

            data = obj.data

        else:
            panic('invalid object record: %r' % (obj,))


        # now we have the data.
        # find out what is oid's serial as of <before state
        try:
            xdata = stor.loadBefore(obj.oid, before)
        except POSKeyError:
            serial_prev = z64
        else:
            if xdata is None:
                serial_prev = z64
            else:
                _, serial_prev, _ = xdata

        # store the object.
        # if it will be ConflictError - we just fail and let the caller retry.
        if data is None:
            stor.deleteObject(obj.oid, serial_prev, txn)
        else:
            stor.store(obj.oid, serial_prev, data, '', txn)

    stor.tpc_vote(txn)

    # in ZODB >= 5 tpc_finish returns tid directly, but on ZODB 4 and ZODB 3 it
    # does not do so. Since we still need to support ZODB 4, utilize tpc_finish
    # callback to know with which tid the transaction was committed.
    _ = []
    stor.tpc_finish(txn, lambda tid: _.append(tid))
    assert len(_) == 1
    tid = _[0]
    return tid


# ----------------------------------------
import sys, getopt

summary = "commit new transaction into a ZODB database"

def usage(out):
    print("""\
Usage: zodb commit [OPTIONS] <storage> <at> < input
Commit new transaction into a ZODB database.

The transaction to be committed is read from stdin in zodbdump format without
first 'txn' header line.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.
<at> is transaction ID of what is caller idea about its current database view.

On success the ID of committed transaction is printed to stdout.

Options:

    -h  --help      show this help
""", file=out)

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

    if len(argv) != 2:
        usage(sys.stderr)
        sys.exit(2)

    storurl = argv[0]
    at = argv[1].decode('hex')

    stor = storageFromURL(storurl)

    zin = 'txn 0000000000000000 " "\n'  # artificial transaction header
    zin += sys.stdin.read()
    zin = BytesIO(zin)
    zr = zodbdump.DumpReader(zin)
    zr.lineno -= 1                      # we prepended txn header
    txn = zr.readtxn()
    tail = zin.read()
    if tail:
        print('E: +%d: garbage after transaction' % zr.lineno, file=sys.stderr)
        sys.exit(1)

    tid = zodbcommit(stor, at, txn)
    print(ashex(tid))
