# -*- coding: utf-8 -*-
# Copyright (C) 2019-2023 Nexedi SA and Contributors.
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

from zodbtools.zodbanalyze import analyze, report
from zodbtools.test.testutil import fs1_testdata_py23
from zodbtools.util import fromhex
import os.path
from golang import b


def test_zodbanalyze(tmpdir, capsys):
    tfs1 = fs1_testdata_py23(tmpdir,
                    os.path.join(os.path.dirname(__file__), "testdata", "1.fs"))

    for use_dbm in (False, True):
        report(
            analyze(
                tfs1,
                use_dbm=use_dbm,
                delta_fs=False,
                tidmin=None,
                tidmax=None,
            ),
            csv=False,
        )
        captured = capsys.readouterr()
        assert "Processed 68 records in 65 transactions" in captured.out
        assert captured.err == ""

    # csv output
    report(
        analyze(
            tfs1,
            use_dbm=False,
            delta_fs=False,
            tidmin=None,
            tidmax=None,
        ),
        csv=True,
    )
    captured = capsys.readouterr()
    assert (
        """Class Name,T.Count,T.Bytes,Pct,AvgSize,C.Count,C.Bytes,O.Count,O.Bytes
persistent.mapping.PersistentMapping,3,639,23.194192%,213.000000,1,213,2,426
__main__.Object,63,2116,76.805808%,33.587302,9,303,54,1813
"""
        == captured.out
    )
    assert captured.err == ""

    # empty range
    report(
        analyze(
            tfs1,
            use_dbm=False,
            delta_fs=False,
            tidmin=fromhex("ffffffffffffffff"),
            tidmax=None,
        ),
        csv=False,
    )
    captured = capsys.readouterr()
    assert "# ø\nNo transactions processed\n" == b(captured.out)
    assert captured.err == ""
