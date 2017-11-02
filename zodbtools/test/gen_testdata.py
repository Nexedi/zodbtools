#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017  Nexedi SA and Contributors.
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
"""generate reference database and index for tests"""

# NOTE result of this script must be saved in version control and should not be
# generated at the time when tests are run. This is because even though we make
# time and random predictable ZODB cannot generally save same transaction
# extension dictionary to the same raw data.
#
# Quoting
#
#   https://docs.python.org/2.7/library/stdtypes.html#dict.items    and
#   https://docs.python.org/3.7/library/stdtypes.html#dictionary-view-objects
#
# """ CPython implementation detail: Keys and values are listed in an arbitrary
#     order which is non-random, varies across Python implementations, and depends
#     on the dictionary’s history of insertions and deletions. """

# NOTE as of 14 Mar 2017 FileStorage cannot commit transactions with non-ASCII
#      metadata - so it is not tested

from ZODB.FileStorage import FileStorage
from ZODB import DB
from ZODB.POSException import UndoError
from persistent import Persistent
import transaction

import sys
import struct
import time
import random
import logging

# convert numeric oid to/from str
def p64(num):
    return struct.pack('>Q', num)

def unpack64(packed):
    return struct.unpack('>Q', packed)[0]

def hex64(packed):
    return '0x%016x' % unpack64(packed)

# make time.time() predictable
_xtime = time.mktime(time.strptime("04 Jan 1979", "%d %b %Y"))
def xtime():
    global _xtime
    _xtime += 1.1
    return _xtime
time.time = xtime


# prepare transaction for a commit
def precommit(user, description, extension):
    txn = transaction.get()
    txn.user = user
    txn.description = description
    txn.extension = extension
    return txn

def commit(user, description, extension):
    txn = precommit(user, description, extension)
    txn.commit()


class Object(Persistent):
    # .value
    def __init__(self, value):
        self.value = value

    def __getstate__(self):
        return self.value

    def __setstate__(self, state):
        self.value = state

# prepare extension dictionary for subject
alnum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def ext(subj):
    d = {"x-generator": "zodb/py%s (%s)" % (sys.version_info.major, subj)}

    # also add some random 'x-cookie'
    cooklen = 5
    cookie = ""
    for _ in range(cooklen):
        cookie += random.choice(alnum)

    xcookie = "x-cookie" + random.choice(alnum)
    d[xcookie] = cookie

    # shufle extension dict randomly - to likely trigger different ordering on save
    keyv = d.keys()
    random.shuffle(keyv)
    ext = {}
    for key in keyv:
        ext[key] = d[key]

    return ext

# gen_testdb generates test FileStorage database @ outfs_path
def gen_testdb(outfs_path):
    logging.basicConfig()

    # generate random changes to objects hooked to top-level root by a/b/c/... key
    random.seed(0)

    namev = [_ for _ in "abcdefg"]
    Niter = 2
    for i in range(Niter):
        stor = FileStorage(outfs_path, create=(i == 0))
        db   = DB(stor)
        conn = db.open()
        root = conn.root()
        assert root._p_oid == p64(0), repr(root._p_oid)

        for j in range(25):
            name = random.choice(namev)
            if name in root:
                obj = root[name]
            else:
                root[name] = obj = Object(None)

            obj.value = "%s%i.%i" % (name, i, j)

            commit(u"user%i.%i" % (i,j), u"step %i.%i" % (i, j), ext(name))

        # undo a transaction one step before a latest one a couple of times
        for j in range(2):
            # XXX undoLog, despite what its interface says:
            #   https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/interfaces.py#L472
            # just returns log of all transactions in specified range:
            #   https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/FileStorage/FileStorage.py#L1008
            #   https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/FileStorage/FileStorage.py#L2103
            # so we retry undoing next log's txn on conflict.
            for ul in db.undoLog(1, 20):
                try:
                    db.undo(ul["id"])
                    commit(u"root%i.%i\nYour\nMagesty " % (i, j),
                           u"undo %i.%i\nmore detailed description\n\nzzz ..." % (i, j) + "\t"*(i+j),
                           ext("undo %s" % ul["id"]))
                except UndoError:
                    transaction.abort()
                    continue

                break

        # delete an object
        name = random.choice(root.keys())
        obj = root[name]
        root[name] = Object("%s%i*" % (name, i))
        # NOTE user/ext are kept empty on purpose - to also test this case
        commit(u"", u"predelete %s" % unpack64(obj._p_oid), {})

        # XXX obj in db could be changed by above undo, but ZODB does not automatically
        # propagate undo changes to live objects - so obj._p_serial can be stale.
        # Get serial via history.
        obj_tid_lastchange = db.history(obj._p_oid)[0]['tid']

        txn = precommit(u"root%i\nYour\nRoyal\nMagesty' " % i +
                            ''.join(chr(_) for _ in range(32)),     # <- NOTE all control characters
                        u"delete %i\nalpha beta gamma'delta\"lambda\n\nqqq ..." % i,
                        ext("delete %s" % unpack64(obj._p_oid)))
        stor.tpc_begin(txn)
        stor.deleteObject(obj._p_oid, obj_tid_lastchange, txn)
        stor.tpc_vote(txn)
        # TODO different txn status vvv
        # XXX vvv it does the thing, but py fs iterator treats this txn as EOF
        #if i != Niter-1:
        #    stor.tpc_finish(txn)
        stor.tpc_finish(txn)

        # close db & rest not to get conflict errors after we touched stor
        # directly a bit. everything will be reopened on next iteration.
        conn.close()
        db.close()
        stor.close()

# ----------------------------------------

from zodbtools.zodbdump import zodbdump

def main():
    out = "testdata/1"
    gen_testdb("%s.fs" % out)
    stor = FileStorage("%s.fs" % out, read_only=True)
    with open("%s.zdump.ok" % out, "w") as f:
        zodbdump(stor, None, None, out=f)

if __name__ == '__main__':
    main()
