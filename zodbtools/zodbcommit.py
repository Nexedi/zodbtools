# Copyright (C) 2018-2022  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
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
from zodbtools.util import ashex, fromhex, storageFromURL, asbinstream
from ZODB.interfaces import IStorageRestoreable
from ZODB.utils import p64, u64, z64
from ZODB.POSException import POSKeyError
from ZODB._compat import BytesIO
from golang import func, defer, panic, b
import warnings


# zodbcommit commits new transaction into ZODB storage with data specified by
# zodbdump transaction.
#
# txn.tid acts as a flag:
# - with tid=0 the transaction is committed regularly.
# - with tid=!0 the transaction is recreated with exactly that tid and txn.status via IStorageRestoreable.
#
# tid of created transaction is returned.
_norestoreWarned = set() # of storage class
def zodbcommit(stor, at, txn):
    assert isinstance(txn, zodbdump.Transaction)

    want_restore = (txn.tid != z64)
    have_restore = IStorageRestoreable.providedBy(stor)
    # warn once if stor does not implement IStorageRestoreable
    if want_restore and (not have_restore):
        if type(stor) not in _norestoreWarned:
            warnings.warn("restore: %s does not provide IStorageRestoreable ...\n"
                          "\t... -> will try to emulate it on best-effort basis." %
                          type(stor), RuntimeWarning)
            _norestoreWarned.add(type(stor))

    if want_restore:
        # even if stor might be not providing IStorageRestoreable and not
        # supporting .restore, it can still support .tpc_begin(tid=...). An example
        # of this is NEO. We anyway need to be able to specify which transaction ID
        # we need to restore transaction with.
        stor.tpc_begin(txn, tid=txn.tid, status=txn.status)
        runctx = "%s: restore %s @%s" % (stor.getName(), ashex(txn.tid), ashex(at))
    else:
        stor.tpc_begin(txn)
        runctx = "%s: commit @%s" % (stor.getName(), ashex(at))

    def _():
        def current_serial(oid):
            return _serial_at(stor, oid, at)
        for obj in txn.objv:
            data = None # data do be committed - setup vvv
            copy_from = None
            if isinstance(obj, zodbdump.ObjectCopy):
                copy_from = obj.copy_from
                try:
                    xdata = stor.loadBefore(obj.oid, p64(u64(obj.copy_from)+1))
                except POSKeyError:
                    xdata = None
                if xdata is None:
                    raise ValueError("%s: object %s: copy from @%s: no data" %
                            (runctx, ashex(obj.oid), ashex(obj.copy_from)))
                data, _, _ = xdata

            elif isinstance(obj, zodbdump.ObjectDelete):
                data = None

            elif isinstance(obj, zodbdump.ObjectData):

                if isinstance(obj.data, zodbdump.HashOnly):
                    raise ValueError('%s: cannot commit transaction with hashonly object' % runctx)

                data = obj.data

            else:
                panic('%s: invalid object record: %r' % (runctx, obj,))

            # we have the data -> restore/store the object.
            # if it will be ConflictError - we just fail and let the caller retry.
            if data is None:
                stor.deleteObject(obj.oid, current_serial(obj.oid), txn)
            else:
                if want_restore and have_restore:
                    stor.restore(obj.oid, txn.tid, data, '', copy_from, txn)
                else:
                    # FIXME we don't handle copy_from on commit
                    # NEO does not support restore, and even if stor supports restore,
                    # going that way requires to already know tid for transaction we are
                    # committing. -> we just imitate copy by actually copying data and
                    # letting the storage deduplicate it.
                    stor.store(obj.oid, current_serial(obj.oid), data, '', txn)

    try:
        _()
        stor.tpc_vote(txn)
    except:
        stor.tpc_abort(txn)
        raise

    # in ZODB >= 5 tpc_finish returns tid directly, but on ZODB 4 it
    # does not do so. Since we still need to support ZODB 4, utilize tpc_finish
    # callback to know with which tid the transaction was committed.
    _ = []
    stor.tpc_finish(txn, lambda tid: _.append(tid))
    assert len(_) == 1
    tid = _[0]
    if want_restore and (tid != txn.tid):
        panic('%s: restored transaction has different tid=%s' % (runctx, ashex(tid)))
    return tid

# _serial_at returns oid's serial as of @at database state.
def _serial_at(stor, oid, at):
    before = p64(u64(at)+1)
    try:
        xdata = stor.loadBefore(oid, before)
    except POSKeyError:
        serial = z64
    else:
        if xdata is None:
            serial = z64
        else:
            _, serial, _ = xdata
    return serial


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
""" + (_low_level_note % "zodb commit"), file=out)

_low_level_note = """
Note: `%s` is low-level tool that creates transactions without checking
data for correctness and consistency at object level. Given incorrect data it
can create transactions with objects that cannot be unpickled, or with objects
that reference other objects that may not already be present in the database.
Such transactions would lead to errors later when accessing those objects at
application level.
"""

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

    if len(argv) != 2:
        usage(sys.stderr)
        sys.exit(2)

    storurl = argv[0]
    at = fromhex(argv[1])

    stor = storageFromURL(storurl)
    defer(stor.close)

    # artificial transaction header with tid=0 to request regular commit
    zin = b('txn 0000000000000000 " "\n')

    zin += asbinstream(sys.stdin).read()
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
