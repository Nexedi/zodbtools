# -*- coding: utf-8 -*-
# Copyright (C) 2016-2018  Nexedi SA and Contributors.
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
"""Zodbcmp - Tool to compare two ZODB databases

Zodbcmp compares two ZODB databases in between tidmin..tidmax transaction range
with default range being 0..+∞ - (whole database).

For comparison both databases are scanned at storage layer and every
transaction content is compared bit-to-bit between the two. The program stops
either at first difference found, or when whole requested transaction range is
scanned with no difference detected.

Exit status is 0 if inputs are the same, 1 if different, 2 if error.
"""

from __future__ import print_function
from zodbtools.util import ashex, inf, nextitem, txnobjv, parse_tidrange, TidRangeInvalid,  \
        storageFromURL
from time import time
from golang import func, defer

# compare two storage transactions
# 0 - equal, 1 - non-equal
def txncmp(txn1, txn2):
    # metadata
    for attr in ('tid', 'status', 'user', 'description', 'extension'):
        attr1 = getattr(txn1, attr)
        attr2 = getattr(txn2, attr)
        if attr1 != attr2:
            return 1

    # data
    objv1 = txnobjv(txn1)
    objv2 = txnobjv(txn2)
    if len(objv1) != len(objv2):
        return 1

    for obj1, obj2 in zip(objv1, objv2):
        for attr in ('oid', 'data', 'data_txn'):
            attr1 = getattr(obj1, attr)
            attr2 = getattr(obj2, attr)
            if attr1 != attr2:
                return 1

    return 0


# compare two storages
# 0 - equal, 1 - non-equal
def storcmp(stor1, stor2, tidmin, tidmax, verbose=False):
    iter1 = stor1.iterator(tidmin, tidmax)
    iter2 = stor2.iterator(tidmin, tidmax)

    Tprev = time()
    txncount = 0
    while 1:
        txn1, ok1 = nextitem(iter1)
        txn2, ok2 = nextitem(iter2)

        # comparison finished
        if not ok1 and not ok2:
            if verbose:
                print("equal")
            return 0

        # one part has entry not present in another part
        if txn1 is None or txn2 is None or txn1.tid != txn2.tid:
            if verbose:
                tid1 = txn1.tid if txn1 else inf
                tid2 = txn2.tid if txn2 else inf
                l = [(tid1, 1,2), (tid2, 2,1)]
                l.sort()
                mintid, minstor, maxstor = l[0]
                print("not-equal: tid %s present in stor%i but not in stor%i" % (
                        ashex(mintid), minstor, maxstor))
            return 1

        # show current comparison state and speed
        if verbose:
            txncount += 1
            T = time()
            if T - Tprev > 5:
                print("@ %s  (%.2f TPS)" % (ashex(txn1.tid), txncount / (T - Tprev)))
                Tprev = T
                txncount = 0

        # actual txn comparison
        tcmp = txncmp(txn1, txn2)
        if tcmp:
            if verbose:
                print("not-equal: transaction %s is different")
            return 1


# ----------------------------------------
import sys, getopt
import traceback

summary = "compare two ZODB databases"

def usage(out):
    print("""\
Usage: zodb cmp [OPTIONS] <storage1> <storage2> [tidmin..tidmax]
Compare two ZODB databases.

<storageX> is an URL (see 'zodb help zurl') of a ZODB-storage.
<tidrange> is a history range (see 'zodb help tidrange') to compare.

Options:

    -v  --verbose   increase verbosity
    -h  --help      show this help
""", file=out)

@func
def main2(argv):
    verbose = False

    try:
        optv, argv = getopt.getopt(argv[1:], "hv", ["help", "verbose"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        if opt in ("-v", "--verbose"):
            verbose = True

    try:
        storurl1, storurl2 = argv[0:2]
    except ValueError:
        usage(sys.stderr)
        sys.exit(2)

    # parse tidmin..tidmax
    tidmin = tidmax = None
    if len(argv) > 2:
        try:
            tidmin, tidmax = parse_tidrange(argv[2])
        except TidRangeInvalid as e:
            print("E: invalid tidrange: %s" % e, file=sys.stderr)
            sys.exit(2)

    stor1 = storageFromURL(storurl1, read_only=True);   defer(stor1.close)
    stor2 = storageFromURL(storurl2, read_only=True);   defer(stor2.close)

    zcmp = storcmp(stor1, stor2, tidmin, tidmax, verbose)
    sys.exit(1 if zcmp else 0)

def main(argv):
    try:
        main2(argv)
    except SystemExit:
        raise   # this was sys.exit() call, not an error
    except:
        traceback.print_exc()
        sys.exit(2)
