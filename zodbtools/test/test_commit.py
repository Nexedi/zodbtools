# -*- coding: utf-8 -*-
# Copyright (C) 2018-2024  Nexedi SA and Contributors.
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
from zodbtools.util import storageFromURL, sha1, ashex, fromhex, BytesIO
from ZODB.utils import p64, u64, z64
from ZODB._compat import dumps, _protocol   # XXX can't yet commit with arbitrary ext.bytes

from golang import func, defer, b
from golang.gcompat import qq

import sys
from subprocess import Popen, PIPE


# verify zodbcommit.
@func
def test_zodbcommit(zsrv, zext):
    stor = storageFromURL(zsrv.zurl)
    defer(stor.close)

    at0 = stor.lastTransaction()

    # commit some transactions via zodbcommit and verify if storage dump gives
    # what is expected.
    t1 = Transaction(z64, ' ', b'user name', b'description ...', zext(dumps({'a': 'b'}, _protocol)), [
        ObjectData(p64(1), b'data1', b('sha1'), sha1(b'data1')),
        ObjectData(p64(2), b'data2', b('sha1'), sha1(b'data2'))])

    t1.tid = zodbcommit(stor, at0, t1)

    t2 = Transaction(z64, ' ', b'user2', b'desc2', b'', [
        ObjectDelete(p64(2))])

    t2.tid = zodbcommit(stor, t1.tid, t2)


    buf = BytesIO()
    zodbdump(stor, p64(u64(at0)+1), None, out=buf)
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


# verify zodbcommit via commandline / stdin.
def test_zodbcommit_cmd(zsrv, zext):
    # for ZEO sync is noop unless server_sync=True is specified in options,
    # but server_sync is available only on ZEO5, not ZEO4. Work it around with
    # opening/closing the storage on every query.
    @func
    def zsrv_do(f):
        stor = storageFromURL(zsrv.zurl)
        defer(stor.close)
        return f(stor)
    at0 = zsrv_do(lambda stor: stor.lastTransaction())

    # zodbcommit_cmd does `zodb commit` via command line and returns TID of
    # committed transction.
    def zodbcommit_cmd(at, stdin): # -> tid
        p = Popen([sys.executable, '-m', 'zodbtools.zodb', 'commit',
                   zsrv.zurl, ashex(at)], stdin=PIPE, stdout=PIPE)
        stdout, _ = p.communicate(stdin)
        assert p.returncode == 0,  stdout
        return fromhex(stdout.rstrip())

    t1 = b'''\
user "user name"
description "description ..."
extension %s
obj 0000000000000001 5 sha1:%s
data1
obj 0000000000000002 5 sha1:%s
data2

''' % (qq(zext(dumps({'a': 'b'}, _protocol))), ashex(sha1(b'data1')), ashex(sha1(b'data2')))

    t2 = b'''\
user "user2"
description "desc2"
extension ""
obj 0000000000000002 delete

'''

    at1 = zodbcommit_cmd(at0, t1)
    at2 = zodbcommit_cmd(at1, t2)

    t1 = (b'txn %s " "\n' % ashex(at1)) + t1
    t2 = (b'txn %s " "\n' % ashex(at2)) + t2

    def _(stor):
        buf = BytesIO()
        zodbdump(stor, p64(u64(at0)+1), None, out=buf)
        return buf.getvalue()
    dumped = zsrv_do(_)

    assert dumped == b''.join([t1, t2])

    t3 = b'''\
user "user3"
description "desc3"
extension ""
obj 0000000000000001 from %s

''' % ashex(at1)

    # XXX see note about ObjectCopy in test_zodbcommit
    at3 = zodbcommit_cmd(at1, t3)

    def _(stor):
        data1_1, _, _ = stor.loadBefore(p64(1), p64(u64(at1)+1))
        data1_3, _, _ = stor.loadBefore(p64(1), p64(u64(at3)+1))
        assert data1_1 == data1_3
        assert data1_1 == b'data1'  # just in case
    zsrv_do(_)
