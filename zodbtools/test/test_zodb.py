# Copyright (C) 2019  Nexedi SA and Contributors.
#                     Jerome Perrin <jerome@nexedi.com>
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


def test_main(capsys):
    with mock.patch.object(sys, 'argv', ('zodb', )), \
         pytest.raises(SystemExit) as excinfo:
        zodb.main()
    assert "Zodb is a tool for managing ZODB databases." in capsys.readouterr(
    ).err
    assert excinfo.value.args == (2, )


@pytest.mark.parametrize(
    "help_topic",
    tuple(zodb.command_dict) + tuple(help_module.topic_dict))
def test_help(capsys, help_topic):
    with mock.patch.object(sys, 'argv', ('zodb', 'help', help_topic)), \
         pytest.raises(SystemExit) as excinfo:
        zodb.main()
    assert capsys.readouterr().out
    assert "" == capsys.readouterr().err
    assert excinfo.value.args == (0, )
