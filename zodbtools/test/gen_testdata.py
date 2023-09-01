#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017-2023  Nexedi SA and Contributors.
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
from ZODB.Connection import TransactionMetaData
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
_xtime0 = time.mktime(time.strptime("04 Jan 1979", "%d %b %Y"))
def xtime_reset():
    global _xtime
    _xtime = _xtime0
xtime_reset()

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
def ext4subj(subj):
    d = {"x-generator": "zodb/py%s (%s)" % (sys.version_info.major, subj)}

    # also add some random 'x-cookie'
    cooklen = 5
    cookie = ""
    for _ in range(cooklen):
        cookie += random.choice(alnum)

    xcookie = "x-cookie" + random.choice(alnum)
    d[xcookie] = cookie

    # shufle extension dict randomly - to likely trigger different ordering on save
    keyv = list(d.keys())
    random.shuffle(keyv)
    ext = {}
    for key in keyv:
        ext[key] = d[key]

    return ext

# run_with_zodb4py2_compat(f) runs f preserving database compatibility with
# ZODB4/py2, which generates pickles encoded with protocol < 3.
#
# ZODB5 started to use protocol 3 and binary for oids starting from ZODB 5.4.0:
# https://github.com/zopefoundation/ZODB/commit/12ee41c4
# Undo it, while we generate test database.
def run_with_zodb4py2_compat(f):
    import ZODB.ConflictResolution
    import ZODB.Connection
    import ZODB.ExportImport
    import ZODB.FileStorage.FileStorage
    import ZODB._compat
    import ZODB.broken
    import ZODB.fsIndex
    import ZODB.serialize
    binary    = getattr(ZODB.serialize, 'binary', None)
    _protocol = getattr(ZODB.serialize, '_protocol', None)
    Pz4 = 2
    try:
        ZODB.serialize.binary    = bytes
        # XXX cannot change just ZODB._compat._protocol, because many modules
        # do `from ZODB._compat import _protocol` and just `import ZODB`
        # imports many ZODB.X modules. In other words we cannot change
        # _protocol just in one place.
        ZODB.ConflictResolution._protocol       = Pz4
        ZODB.Connection._protocol               = Pz4
        ZODB.ExportImport._protocol             = Pz4
        ZODB.FileStorage.FileStorage._protocol  = Pz4
        ZODB._compat._protocol                  = Pz4
        ZODB.broken._protocol                   = Pz4
        ZODB.fsIndex._protocol                  = Pz4
        ZODB.serialize._protocol                = Pz4

        f()
    finally:
        ZODB.serialize.binary    = binary
        ZODB.ConflictResolution._protocol       = _protocol
        ZODB.Connection._protocol               = _protocol
        ZODB.ExportImport._protocol             = _protocol
        ZODB.FileStorage.FileStorage._protocol  = _protocol
        ZODB._compat._protocol                  = _protocol
        ZODB.broken._protocol                   = _protocol
        ZODB.fsIndex._protocol                  = _protocol
        ZODB.serialize._protocol                = _protocol

# gen_testdb generates test FileStorage database @ outfs_path.
#
# zext indicates whether or not to include non-empty extension into transactions.
def gen_testdb(outfs_path, zext=True):
    def _():
        _gen_testdb(outfs_path, zext)
    run_with_zodb4py2_compat(_)

def _gen_testdb(outfs_path, zext):
    xtime_reset()

    ext = ext4subj
    if not zext:
        def ext(subj): return {}

    logging.basicConfig()

    # generate random changes to objects hooked to top-level root by a/b/c/... key
    random.seed(0)

    namev = [_ for _ in "abcdefg"]
    Niter = 3
    for i in range(Niter):
        stor = FileStorage(outfs_path, create=(i == 0))
        db   = DB(stor)
        if i == 1:
            # change several transactions created during first pass to have "p" status
            # (this also removes some transactions completely)
            db.pack()
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
        name = random.choice(list(root.keys()))
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
        # at low level stor requires ZODB.IStorageTransactionMetaData not txn (ITransaction)
        txn_stormeta = TransactionMetaData(txn.user, txn.description, txn.extension)
        stor.tpc_begin(txn_stormeta)
        stor.deleteObject(obj._p_oid, obj_tid_lastchange, txn_stormeta)
        stor.tpc_vote(txn_stormeta)
        stor.tpc_finish(txn_stormeta)

        # close db & rest not to get conflict errors after we touched stor
        # directly a bit. everything will be reopened on next iteration.
        conn.close()
        db.close()
        stor.close()

# ----------------------------------------

from zodbtools.zodbdump import zodbdump
from zodbtools.test.testutil import zext_supported

def main():
    # check that ZODB supports txn.extension_bytes; refuse to work if not.
    if not zext_supported():
        raise RuntimeError("gen_testdata must be used with ZODB that supports txn.extension_bytes")

    out = "testdata/1"
    for zext in [True, False]:
        dbname = out
        if not zext:
            dbname += "_!zext"
        gen_testdb("%s.fs" % dbname, zext=zext)
        stor = FileStorage("%s.fs" % dbname, read_only=True)
        for pretty in ('raw', 'zpickledis'):
            with open("%s.zdump.%s.ok" % (dbname, pretty), "wb") as f:
                zodbdump(stor, None, None, pretty=pretty, out=f)

if __name__ == '__main__':
    main()
