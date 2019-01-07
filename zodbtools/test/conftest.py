# Copyright (C) 2019  Nexedi SA and Contributors.
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

import pytest
from zodbtools.test.testutil import zext_supported

# zext is a test fixture function object that allows to exercise 2 cases:
#
# - when ZODB does not have txn.extension_bytes support
# - when ZODB might have    txn.extension_bytes support
#
# in a test, zext should be used as as follows:
#
#   def test_something(zext):
#       # bytes for an extension dict
#       raw_ext = dumps({...})
#
#       # will be either same as raw_ext, or b'' if ZODB lacks txn.extension_bytes support
#       raw_ext = zext(raw_ext)
#
#       # zext.disabled indicates whether testing for non-empty extension was disabled.
#       if zext.disabled:
#           ...
@pytest.fixture(params=['!zext', 'zext'])
def zext(request):
    if request.param == '!zext':
        # txn.extension_bytes is not working - always test with empty extension
        def _(ext):
            return b''
        _.disabled = True
        return _
    else:
        # txn.extension_bytes might be working - test with given extension and
        # xfail if ZODB does not have necessary support.
        def _(ext):
            return ext
        _.disabled = False
        if not zext_supported():
            request.applymarker(pytest.mark.xfail(reason='ZODB does not have txn.extension_bytes support'))
        return _
