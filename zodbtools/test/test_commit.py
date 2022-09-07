# -*- coding: utf-8 -*-
# Copyright (C) 2018-2022  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
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

from zodbtools.zodbcommit import zodbcommit
from zodbtools.zodbdump import zodbdump, Transaction, ObjectData, ObjectDelete, ObjectCopy
from zodbtools.util import storageFromURL, sha1
from ZODB.utils import p64, u64, z64
from ZODB._compat import BytesIO, dumps, _protocol   # XXX can't yet commit with arbitrary ext.bytes

from tempfile import mkdtemp
from shutil import rmtree
from golang import func, defer, b

# verify zodbcommit.
@func
def test_zodbcommit(zext):
    tmpd = mkdtemp('', 'zodbcommit.')
    defer(lambda: rmtree(tmpd))

    stor = storageFromURL('%s/2.fs' % tmpd)
    defer(stor.close)

    head = stor.lastTransaction()

    # commit some transactions via zodbcommit and verify if storage dump gives
    # what is expected.
    t1 = Transaction(z64, ' ', b'user name', b'description ...', zext(dumps({'a': 'b'}, _protocol)), [
        ObjectData(p64(1), b'data1', b('sha1'), sha1(b'data1')),
        ObjectData(p64(2), b'data2', b('sha1'), sha1(b'data2'))])

    t1.tid = zodbcommit(stor, head, t1)

    t2 = Transaction(z64, ' ', b'user2', b'desc2', b'', [
        ObjectDelete(p64(2))])

    t2.tid = zodbcommit(stor, t1.tid, t2)


    buf = BytesIO()
    zodbdump(stor, p64(u64(head)+1), None, out=buf)
    dumped = buf.getvalue()

    assert dumped == b''.join([_.zdump() for _ in (t1, t2)])

    # ObjectCopy. XXX zodbcommit handled ObjectCopy by actually copying data,
    # not referencing previous transaction via backpointer.
    t3 = Transaction(z64, ' ', b'user3', b'desc3', b'', [
        ObjectCopy(p64(1), t1.tid)])

    t3.tid = zodbcommit(stor, t2.tid, t3)

    data1_1, _, _ = stor.loadBefore(p64(1), p64(u64(t1.tid)+1))
    data1_3, _, _ = stor.loadBefore(p64(1), p64(u64(t3.tid)+1))
    assert data1_1 == data1_3
    assert data1_1 == b'data1'  # just in case
