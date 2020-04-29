# -*- coding: utf-8 -*-
# Copyright (C) 2019-2020 Nexedi SA and Contributors.
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

import datetime
import os
import time

import pytest
import pytz
from freezegun import freeze_time
import tzlocal

from zodbtools.util import TidRangeInvalid, TidInvalid, ashex, parse_tid, parse_tidrange
from golang import b


@pytest.fixture
def fake_time():
    """Pytest's fixture to run this test as if now() was 2009-08-30T19:20:00Z
    and if the machine timezone was Europe/Paris
    """
    initial_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Paris"
    time.tzset()
    tzlocal.reload_localzone()

    reference_time = datetime.datetime(2009, 8, 30, 19, 20, 0, 0,
                                       pytz.utc).astimezone(
                                           pytz.timezone("Europe/Paris"))
    with freeze_time(reference_time):
        yield
    del os.environ["TZ"]
    if initial_tz:
        os.environ["TZ"] = initial_tz
    time.tzset()


def test_tidrange_tid():
    assert (
        b"\x00\x00\x00\x00\x00\x00\xaa\xaa",
        b"\x00\x00\x00\x00\x00\x00\xbb\xbb",
    ) == parse_tidrange("000000000000aaaa..000000000000bbbb")

    assert (b"\x00\x00\x00\x00\x00\x00\xaa\xaa",
            None) == parse_tidrange("000000000000aaaa..")

    assert (None, b"\x00\x00\x00\x00\x00\x00\xbb\xbb"
            ) == parse_tidrange("..000000000000bbbb")

    assert (None, None) == parse_tidrange("..")

    with pytest.raises(TidRangeInvalid) as exc:
        parse_tidrange("inv.alid")
    assert exc.value.args == ("inv.alid", )

    # range is correct, but a TID is invalid
    with pytest.raises(TidInvalid) as exc:
        parse_tidrange("invalid..")
    assert exc.value.args == ("invalid", )


def test_tidrange_date():
    assert (
        b"\x03\xc4\x85v\x00\x00\x00\x00",
        b"\x03\xc4\x88\xa0\x00\x00\x00\x00",
    ) == parse_tidrange(
        "2018-01-01T10:30:00Z..2018-01-02T00:00:00.000000+00:00")


def test_parse_tid():
    assert b"\x00\x00\x00\x00\x00\x00\xbb\xbb" == parse_tid("000000000000bbbb")

    with pytest.raises(TidInvalid) as exc:
        parse_tid("invalid")
    assert exc.value.args == ("invalid", )

    with pytest.raises(TidInvalid) as exc:
        parse_tid('')
    assert exc.value.args == ('', )


test_parameters = [] # of (reference_time, reference_tid, input_time)
with open(
        os.path.join(
            os.path.dirname(__file__), "testdata",
            "tid-time-format.txt")) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            test_parameters.append(line.split(" ", 2))


@pytest.mark.parametrize("reference_time,reference_tid,input_time",
                         test_parameters)
def test_parse_tid_time_format(fake_time, reference_time, reference_tid,
                               input_time):
    assert b(reference_tid) == ashex(parse_tid(input_time))
    # check that the reference_tid matches the reference time, mainly
    # to check that input is defined correctly.
    assert b(reference_tid) == ashex(parse_tid(reference_time))
