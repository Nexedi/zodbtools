# Copyright (C) 2017-2018  Nexedi SA and Contributors.
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

from zodbtools.zodbdump import (
        zodbdump, DumpReader, Transaction, ObjectDelete, ObjectCopy,
        ObjectData, HashOnly
    )
from ZODB.FileStorage import FileStorage
from ZODB.utils import p64
from cStringIO import StringIO

from os.path import dirname

from pytest import raises

# verify zodbdump output against golden
def test_zodbdump():
    tdir = dirname(__file__)
    stor = FileStorage('%s/testdata/1.fs' % tdir, read_only=True)

    with open('%s/testdata/1.zdump.ok' % tdir) as f:
        dumpok = f.read()

    out = StringIO()
    zodbdump(stor, None, None, out=out)

    assert out.getvalue() == dumpok


# verify zodbdump.DumpReader
def test_dumpreader():
    in_ = b"""\
txn 0123456789abcdef " "
user "my name"
description "o la-la..."
extension "zzz123 def"
obj 0000000000000001 delete
obj 0000000000000002 from 0123456789abcdee
obj 0000000000000003 54 adler32:01234567 -
obj 0000000000000004 4 sha1:9865d483bc5a94f2e30056fc256ed3066af54d04
ZZZZ
obj 0000000000000005 9 crc32:52fdeac5
ABC

DEF!

txn 0123456789abcdf0 " "
user "author2"
description "zzz"
extension "qqq"

"""

    r = DumpReader(StringIO(in_))
    t1 = r.readtxn()
    assert isinstance(t1, Transaction)
    assert t1.tid == '0123456789abcdef'.decode('hex')
    assert t1.user              == b'my name'
    assert t1.description       == b'o la-la...'
    assert t1.extension_bytes   == b'zzz123 def'
    assert len(t1.objv)         == 5
    _ = t1.objv[0]
    assert isinstance(_, ObjectDelete)
    assert _.oid        == p64(1)
    _ = t1.objv[1]
    assert isinstance(_, ObjectCopy)
    assert _.oid        == p64(2)
    assert _.copy_from  == '0123456789abcdee'.decode('hex')
    _ = t1.objv[2]
    assert isinstance(_, ObjectData)
    assert _.oid        == p64(3)
    assert _.data       == HashOnly(54)
    assert _.hashfunc   == 'adler32'
    assert _.hash_      == '01234567'.decode('hex')
    _ = t1.objv[3]
    assert isinstance(_, ObjectData)
    assert _.oid        == p64(4)
    assert _.data       == b'ZZZZ'
    assert _.hashfunc   == 'sha1'
    assert _.hash_      == '9865d483bc5a94f2e30056fc256ed3066af54d04'.decode('hex')
    _ = t1.objv[4]
    assert isinstance(_, ObjectData)
    assert _.oid        == p64(5)
    assert _.data       == b'ABC\n\nDEF!'
    assert _.hashfunc   == 'crc32'
    assert _.hash_      == '52fdeac5'.decode('hex')

    t2 = r.readtxn()
    assert isinstance(t2, Transaction)
    assert t2.tid == '0123456789abcdf0'.decode('hex')
    assert t2.user              == b'author2'
    assert t2.description       == b'zzz'
    assert t2.extension_bytes   == b'qqq'
    assert t2.objv              == []

    assert r.readtxn() == None

    z = ''.join([_.zdump() for _ in (t1, t2)])
    assert z == in_

    # unknown hash function
    r = DumpReader(StringIO("""\
txn 0000000000000000 " "
user ""
description ""
extension ""
obj 0000000000000001 1 xyz:0123 -

"""))
    with raises(RuntimeError) as exc:
        r.readtxn()
    assert exc.value.args == ("""+5: invalid line: unknown hash function "xyz" ('obj 0000000000000001 1 xyz:0123 -')""",)

    # data integrity error
    r = DumpReader(StringIO("""\
txn 0000000000000000 " "
user ""
description ""
extension ""
obj 0000000000000001 5 crc32:01234567
hello

"""))
    with raises(RuntimeError) as exc:
        r.readtxn()
    assert exc.value.args == ("""+6: data corrupt: crc32 = 3610a686, expected 01234567""",)
