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
    report(
        analyze(
            os.path.join(os.path.dirname(__file__), 'testdata', '1.fs'),
            use_dbm=False,
            delta_fs=False,
            tidmin=None,
            tidmax=None),
        csv=False)
    captured = capsys.readouterr()
    assert "Processed 68 records in 59 transactions" in captured.out
    assert captured.err == ""