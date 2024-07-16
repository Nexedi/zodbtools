#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017-2024  Nexedi SA and Contributors.
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
"""generate reference FileStorage databases and indices for tests

We generate test database with all, potentially tricky, cases of transaction,
data and index records. This database is generated multiple times with
different ZODB settings that try to mimic what notable ZODB versions would
produce. The following combinations are covered:

    py2: ZODB 4 and ZODB5 < 5.3     (pickle protocol 1)
    py2: ZODB 5.3                   (pickle protocol 2)
    py2: ZODB ≥ 5.4                 (pickle protocol 3)

Each such combination is referred to by "zkind" which indicates major Python
and pickle protocol versions used, for example "py2_pickle3". See
run_with_all_zodb_pickle_kinds function for details.

Golden zodbdump & zodbanalyze outputs are also generated besides databases themselves.
"""

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

# NOTE besides zodbtools this module is also used in ZODB/go and in Wendelin.core .

from ZODB.FileStorage import FileStorage
from ZODB import DB
from ZODB.Connection import TransactionMetaData
from ZODB.POSException import UndoError
from persistent import Persistent
import transaction

import os
import os.path
import sys
import shutil
import struct
import time
import random2
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

# rand is our private PRNG.
# It is made independent to stay predictable even if third-party code uses random as well.
# It also provides the same behaviour for both py2 and py3 so that generated
# test data closely match each other where possible on all python versions.
rand = random2.Random()
del random2
def _():  # assert that rand behaviour is predictable
    rand.seed(0)
    R = lambda: rand.randint(0, 99)
    v = list(R() for _ in range(10))
    assert v == [84, 75, 42, 25, 51, 40, 78, 30, 47, 58],   v
    rand.shuffle(v)
    assert v == [84, 47, 30, 78, 75, 25, 40, 42, 51, 58],   v
    y = list(rand.choice(v) for _ in v)
    assert y == [58, 78, 42, 51, 40, 75, 47, 75, 40, 58],   y
_()

# keys returns list of obj.keys() in predictable order independent of python version.
def keys(obj):
    vk = list(obj.keys())
    vk.sort()
    return vk

# prepare extension dictionary for subject
alnum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def ext4subj(subj):
    d = {"x-generator": "zodb/py%s (%s)" % (sys.version_info.major, subj)}

    # also add some random 'x-cookie'
    cooklen = 5
    cookie = ""
    for _ in range(cooklen):
        cookie += rand.choice(alnum)

    xcookie = "x-cookie" + rand.choice(alnum)
    d[xcookie] = cookie

    # shufle extension dict randomly - to likely trigger different ordering on save
    keyv = keys(d)
    rand.shuffle(keyv)
    ext = {}
    for key in keyv:
        ext[key] = d[key]

    return ext


# run_with_all_zodb_pickle_kinds runs f for all ZODB pickle kinds we care about.
#
# For each kind f is run separately under corresponding environment.
# We currently support the following kinds:
#
#   py2: ZODB with pickle protocol = 1      generated by ZODB4 and ZODB5 < 5.3
#   py2: ZODB with pickle protocol = 2      generated by ZODB5 5.3
#   py2: ZODB with pickle protocol = 3      generated by ZODB5 ≥ 5.4
#
# For convenience f can detect under which environment it is being executed via current_zkind.
#
# NOTE only the kinds supported under current python are executed.
def run_with_all_zodb_pickle_kinds(f):
    # NOTE keep in sync with ztestdata fixture.
    def _(expect_protocol=None):
        from ZODB import serialize as zserialize
        if expect_protocol is not None:
            assert zserialize._protocol == expect_protocol, (current_zkind(), expect_protocol)
        f()
    _run_with_zodb4py2_compat(_, 1)
    _run_with_zodb4py2_compat(_, 2)
    _(3)

# current_zkind returns string indicating currently activated ZODB environment,
# for example "py2_pickle3".
def current_zkind():
    from ZODB import serialize as zserialize
    zkind = "py%d_pickle%d" % (sys.version_info.major, zserialize._protocol)
    return zkind

# _run_with_zodb4py2_compat runs f preserving database compatibility with
# ZODB4/py2, which generates pickles encoded with protocol < 3.
#
# ZODB5 started to use protocol 3 and binary for oids starting from ZODB 5.4.0:
# https://github.com/zopefoundation/ZODB/commit/12ee41c4
# Undo it, while we generate test database as if produced by older ZODB.
def _run_with_zodb4py2_compat(f, protocol):
    assert protocol < 3
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
    try:
        ZODB.serialize.binary    = bytes
        # XXX cannot change just ZODB._compat._protocol, because many modules
        # do `from ZODB._compat import _protocol` and just `import ZODB`
        # imports many ZODB.X modules. In other words we cannot change
        # _protocol just in one place.
        ZODB.ConflictResolution._protocol       = protocol
        ZODB.Connection._protocol               = protocol
        ZODB.ExportImport._protocol             = protocol
        ZODB.FileStorage.FileStorage._protocol  = protocol
        ZODB._compat._protocol                  = protocol
        ZODB.broken._protocol                   = protocol
        ZODB.fsIndex._protocol                  = protocol
        ZODB.serialize._protocol                = protocol

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
    xtime_reset()

    def ext(subj):
        # invoke ext4subj for both zext and !zext so that PRNG is left in the same state for both cases
        e = ext4subj(subj)
        if not zext:
            e = {}
        return e

    logging.basicConfig()

    # generate random changes to objects hooked to top-level root by a/b/c/... key
    rand.seed(0)

    namev = [_ for _ in "abcdefg"]
    Niter = 3
    nameobj1 = None  # name used when adding first object
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
            name = rand.choice(namev)
            if name in root:
                obj = root[name]
            else:
                root[name] = obj = Object(None)

            obj.value = "%s%i.%i" % (name, i, j)

            commit(u"user%i.%i" % (i,j), u"step %i.%i" % (i, j), ext(name))

            if nameobj1 is None:
                nameobj1 = name
                assert obj._p_oid == p64(1), repr(obj._p_oid)

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

        # create a cyclic object -> object reference
        # pretty=zpickledis used not to handle this well because in ZODB pickle the reference
        # referes to referred type by GET that is prepared by PUT in class part of the pickle.
        name = rand.choice(keys(root))
        obj = root[name]
        obj.value = obj
        commit(u"user", u"cyclic reference", ext("cycle"))

        # delete an object
        _ = keys(root);  _.remove(nameobj1)  # preserve the first obj not to go
        name = rand.choice(_)
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
from zodbtools import zodbanalyze
from zodbtools.test.testutil import zext_supported

def main():
    # check that ZODB supports txn.extension_bytes; refuse to work if not.
    if not zext_supported():
        raise RuntimeError("gen_testdata must be used with ZODB that supports txn.extension_bytes")

    top = "testdata/1"
    def _():
        for zext in [True, False]:
            prefix = "%s%s/%s" % (top, "" if zext else "_!zext", current_zkind())
            if os.path.exists(prefix):
                shutil.rmtree(prefix)
            os.makedirs(prefix)

            outfs = "%s/data.fs" % prefix
            gen_testdb(outfs, zext=zext)

            # prepare zdump.ok for generated database
            stor = FileStorage(outfs, read_only=True)
            for pretty in ('raw', 'zpickledis'):
                with open("%s/zdump.%s.ok" % (prefix, pretty), "wb") as f:
                    zodbdump(stor, None, None, pretty=pretty, out=f)

            # prepare zanalyze.csv.ok
            sys_stdout = sys.stdout
            sys.stdout = open("%s/zanalyze.csv.ok" % prefix, "w")
            zodbanalyze.report(
                zodbanalyze.analyze(outfs, use_dbm=False, delta_fs=False, tidmin=None, tidmax=None),
                csv=True,
            )
            sys.stdout.close()
            sys.stdout = sys_stdout

    run_with_all_zodb_pickle_kinds(_)


if __name__ == '__main__':
    main()
