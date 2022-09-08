#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2019-2022  Nexedi SA and Contributors.
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
"""utilities for testing"""

from ZODB.FileStorage import FileStorage
from ZODB import DB
import transaction

from tempfile import mkdtemp
from shutil import rmtree
from golang import func, defer
from six import PY3
from os.path import basename

from zodbtools.util import readfile, writefile

# zext_supported checks whether ZODB supports txn.extension_bytes .
_zext_supported_memo = None
def zext_supported():
    global _zext_supported_memo
    if _zext_supported_memo is not None:
        return _zext_supported_memo

    _ = _zext_supported_memo = _zext_supported()
    return _

@func
def _zext_supported():
    tmpd = mkdtemp('', 'zext_check.')
    defer(lambda: rmtree(tmpd))
    dbfs = tmpd + '/1.fs'

    stor = FileStorage(dbfs, create=True)
    db   = DB(stor)
    conn = db.open()
    root = conn.root()
    root._p_changed = True

    txn = transaction.get()
    txn.setExtendedInfo('a', 'b')
    txn.commit()

    for last_txn in stor.iterator(start=stor.lastTransaction()):
        break
    else:
        assert False, "cannot see details of last transaction"

    assert last_txn.extension == {'a': 'b'}
    return hasattr(last_txn, 'extension_bytes')


# fs1_testdata_py23 prepares and returns path to temprary FileStorage prepared
# from testdata with header adjusted to work on current Python.
def fs1_testdata_py23(tmpdir, path):
    data  = readfile(path)
    index = readfile(path + ".index")
    assert data[:4] == b"FS21"      # FileStorage magic for Python2
    if PY3:
        data = b"FS30" + data[4:]   # FileStorage magic for Python3

    path_ = "%s/%s" % (tmpdir, basename(path))

    writefile(path_, data)
    writefile("%s.index" % path_, index)
    return path_
