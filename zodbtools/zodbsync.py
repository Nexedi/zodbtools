# coding: utf-8
# Copyright (C) 2023  Nexedi SA and Contributors.
#                     Jérome Perrin <jerome@nexedi.com>
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
"""Zodbsync - Replicate a primary ZODB storage into a secondary storage

"""

from __future__ import print_function
import datetime

from zodbtools.zodbcommit import zodbcommit
from zodbtools.zodbdump import Transaction, ObjectCopy, ObjectData, ObjectDelete
from zodbtools.util import ashex, parse_tid, storageFromURL, txnobjv

from ZODB.interfaces import IStorageIteration
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import z64, readable_tid_repr

from golang import func, defer

import logging

logging.basicConfig(level=logging.DEBUG)


def zodbsync(primary_store, secondary_store, until, verbosity):
    at = secondary_store.lastTransaction()
    if verbosity > 1:
        print('primary at', readable_tid_repr(primary_store.lastTransaction()))
        print('secondary at', readable_tid_repr(at))
        print("replicating from", readable_tid_repr(at), end='')
        if until:
            print(" until", readable_tid_repr(until), end='')
        print()

    start = datetime.datetime.now()
    transaction_count = 0
    for t in primary_store.iterator(start=at, stop=until):
        if t.tid <= at and at != z64:
            if verbosity > 2:
                # XXX happens always at least once
                print("skipping already present", readable_tid_repr(at))
            continue

        objv = []
        for obj in t: #(txnobjv(t)):
            if obj.data is None:
                assert not obj.data_txn
                objv.append(ObjectDelete(obj.oid))
            elif obj.data_txn is not None:
                objv.append(ObjectCopy(obj.oid, obj.data_txn, data=obj.data))
            else:
                objv.append(ObjectData(obj.oid, obj.data, 'null', None))

        txn = Transaction(
            t.tid,
            t.status,
            t.user,
            t.description,
            t.extension_bytes,
            objv)
        zodbcommit(secondary_store, at, txn)
        transaction_count += 1


        if verbosity > 0:
            behind = TimeStamp(primary_store.lastTransaction()).timeTime() - TimeStamp(at).timeTime()
            print("behind=%s" % behind)

        if verbosity > 1:
            print(readable_tid_repr(txn.tid), t.user, t.description, len(objv), str(datetime.timedelta(seconds=behind)))
        elif verbosity > 0:
            print(readable_tid_repr(txn.tid))
        at = txn.tid


    if verbosity:
        print("replicated %d transactions in %s" % (
            transaction_count, datetime.datetime.now() - start))


# ----------------------------------------
import sys, getopt

summary = "Replicate a primary ZODB storage into a secondary storage"

def usage(out):
    print("""\
Usage: zodb sync [OPTIONS] <primary-storage> <secondary-storage> [<until>]

Replicate transactions from a primary storage into a secondary storage.

<primary-storage> and <secondary-storage> are URLs (see 'zodb help zurl')
of ZODB-storages.
<until> is transaction ID until when transactions should be copied, this
equivalent to ..<until> tidrange, see 'zodb help tidrange'

Options:

    -v  --verbose   increase verbosity (can be passed multiple times)
    -h  --help      show this help
""", file=out)


@func
def main(argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "hv", ["help", "verbose"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    verbosity = 0
    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        if opt in ("-v", "--verbose"):
            verbosity += 1

    if len(argv) not in (2, 3):
        usage(sys.stderr)
        sys.exit(2)


    primary_store = storageFromURL(argv[0], read_only=True)
    defer(primary_store.close)
    secondary_store = storageFromURL(argv[1])
    defer(secondary_store.close)

    until = parse_tid(argv[2]) if len(argv) == 3 else None

    zodbsync(primary_store, secondary_store, until, verbosity)
