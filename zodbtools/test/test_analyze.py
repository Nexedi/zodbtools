# -*- coding: utf-8 -*-
# Copyright (C) 2019 Nexedi SA and Contributors.
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
import os.path


def test_zodbanalyze(capsys):
    for use_dbm in (False, True):
        report(
            analyze(
                os.path.join(os.path.dirname(__file__), "testdata", "1.fs"),
                use_dbm=use_dbm,
                delta_fs=False,
                tidmin=None,
                tidmax=None,
            ),
            csv=False,
        )
        captured = capsys.readouterr()
        assert "Processed 68 records in 59 transactions" in captured.out
        assert captured.err == ""

    # csv output
    report(
        analyze(
            os.path.join(os.path.dirname(__file__), "testdata", "1.fs"),
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
persistent.mapping.PersistentMapping,10,1578,45.633314%,157.800000,1,213,9,1365
__main__.Object,56,1880,54.366686%,33.571429,9,303,47,1577
"""
        == captured.out
    )
    assert captured.err == ""

    # empty range
    report(
        analyze(
            os.path.join(os.path.dirname(__file__), "testdata", "1.fs"),
            use_dbm=False,
            delta_fs=False,
            tidmin="ffffffffffffffff",
            tidmax=None,
        ),
        csv=False,
    )
    captured = capsys.readouterr()
    assert "# ø\nNo transactions processed\n" == captured.out.encode('utf-8')
    assert captured.err == ""
