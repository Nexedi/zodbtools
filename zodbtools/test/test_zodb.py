# -*- coding: utf-8 -*-
# Copyright (C) 2019  Nexedi SA and Contributors.
#                     Jérome Perrin <jerome@nexedi.com>
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

import sys
try:
    from unittest import mock
except ImportError:
    # BBB python2
    import mock

import pytest

from zodbtools import zodb
from zodbtools import help as help_module


# zodbrun runs zodb.main with argv and returns exit code + captured stdout/stderr.
def zodbrun(capsys, *argv):
    with mock.patch.object(sys, 'argv', ('zodb',) + argv), \
         pytest.raises(SystemExit) as excinfo:
        zodb.main()
    assert len(excinfo.value.args) == 1
    ecode = excinfo.value.args[0]
    return ecode, capsys.readouterr()


def test_main(capsys):
    e, _ = zodbrun(capsys)
    assert e == 2
    assert "" == _.out
    assert "Zodb is a tool for managing ZODB databases." in _.err

    e, _ = zodbrun(capsys, '-h')
    assert e == 0
    assert "Zodb is a tool for managing ZODB databases." in _.out
    assert "" == _.err


@pytest.mark.parametrize(
    "help_topic",
    tuple(zodb.command_dict) + tuple(help_module.topic_dict))
def test_help(capsys, help_topic):
    e, _ = zodbrun(capsys, 'help', help_topic)
    assert e == 0
    assert _.err == ""
    assert _.out != ""
