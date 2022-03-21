# -*- coding: utf-8 -*-
# Copyright (C) 2021  Nexedi SA and Contributors.
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

from __future__ import print_function

from zodbtools.zodbrestore import zodbrestore
from zodbtools.util import storageFromURL

from os.path import dirname
from tempfile import mkdtemp
from shutil import rmtree
from golang import func, defer

# verify zodbrestore.
@func
def test_zodbrestore(zext):
    tmpd = mkdtemp('', 'zodbrestore.')
    defer(lambda: rmtree(tmpd))
    zkind = '_!zext' if zext.disabled else ''

    # restore from testdata/1.zdump.ok and verify it gives result that is
    # bit-to-bit identical to testdata/1.fs
    tdata = dirname(__file__) + "/testdata"
    @func
    def _():
        zdump = open("%s/1%s.zdump.default.ok" % (tdata, zkind), 'rb')
        defer(zdump.close)

        stor = storageFromURL('%s/2.fs' % tmpd)
        defer(stor.close)

        zodbrestore(stor, zdump)
    _()

    zfs1 = _readfile("%s/1%s.fs" % (tdata, zkind))
    zfs2 = _readfile("%s/2.fs" % tmpd)
    assert zfs1 == zfs2


# _readfile reads file at path.
def _readfile(path): # -> data(bytes)
    with open(path, 'rb') as _:
        return _.read()
