# -*- coding: utf-8 -*-
# Copyright (C) 2021-2024  Nexedi SA and Contributors.
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

from __future__ import print_function

from zodbtools.zodbrestore import zodbrestore
from zodbtools.util import storageFromURL, readfile
from zodbtools.test.testutil import fs1_testdata_py23

from pytest import mark
from golang import func, defer

# verify zodbrestore.
@mark.need_zext_support
@func
def test_zodbrestore(tmpdir, ztestdata):
    # restore from zdump.ok and verify it gives result that is
    # bit-to-bit identical to data.fs
    @func
    def _():
        zdump = open("%s/zdump.raw.ok" % ztestdata.prefix, 'rb')
        defer(zdump.close)

        stor = storageFromURL('%s/2.fs' % tmpdir)
        defer(stor.close)

        zodbrestore(stor, zdump)
    _()

    zfs1 = readfile(fs1_testdata_py23(tmpdir, "%s/data.fs" % ztestdata.prefix))
    zfs2 = readfile("%s/2.fs" % tmpdir)
    assert zfs1 == zfs2
