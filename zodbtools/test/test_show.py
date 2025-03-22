# -*- coding: utf-8 -*-
# Copyright (C) 2025 Nexedi SA and Contributors.
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

from zodbtools.zodbshow import zodbshow
from zodbtools.test.testutil import fs1_testdata_py23

import pytest
import six
from ZODB.FileStorage import FileStorage


expected_output_py3 = {
    "0x0b": "{'state': 'simple object'}",
    "\x00\x00\x00\x00\x00\x00\x00\x0b": "{'state': 'simple object'}",
    "0x0c": "PersistentReference(p_oid='0x0d', class_meta=<class '__main__.Object'>)",
    "0x0e": "Instance(class=<class '__main__.NonPersistentObject'>, state={'value': (1, 'two')})",
    "0x0f": "Instance(class=<class 're._compile'>, reduce=('.*', 32))",
}


expected_output_py2 = dict(expected_output_py3)
expected_output_py2["0x0f"] = "Instance(class=<class 're._compile'>, reduce=('.*', 0))"


expected_output_py3_pickle3_on_py2 = dict(expected_output_py3)
expected_output_py3_pickle3_on_py2["0x0b"] = "{u'state': u'simple object'}"
expected_output_py3_pickle3_on_py2["\x00\x00\x00\x00\x00\x00\x00\x0b"] = "{u'state': u'simple object'}"
expected_output_py3_pickle3_on_py2["0x0e"] = "Instance(class=<class '__main__.NonPersistentObject'>, state={u'value': (1, u'two')})"
expected_output_py3_pickle3_on_py2["0x0f"] = "Instance(class=<class 're._compile'>, reduce=(u'.*', 32))"


@pytest.mark.parametrize("oid", expected_output_py3.keys())
def test_zodbshow(tmpdir, ztestdata, capsys, oid):
    if ztestdata.zkind == 'py3_pickle3':
        if six.PY2:
            output = expected_output_py3_pickle3_on_py2[oid]
        else:
            output = expected_output_py3[oid]
    else:
        output = expected_output_py2[oid]

    tfs1 = fs1_testdata_py23(tmpdir, '%s/data.fs' % ztestdata.prefix)
    stor = FileStorage(tfs1, read_only=True)
    zodbshow(stor, oid)
    captured = capsys.readouterr()
    assert captured.out == "<class '__main__.Object'>\n%s\n" % output
    assert captured.err == ""
