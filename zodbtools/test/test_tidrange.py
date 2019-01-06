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

import os
import time
from pytest import raises, fixture
from zodbtools.util import parse_tidrange, TidRangeInvalid, ashex
from ZODB.TimeStamp import TimeStamp


@fixture
def europe_paris_timezone():
    """Pytest's fixture to run this test with Europe/Paris as default timezone.
    """
    initial_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Paris"
    time.tzset()
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

    assert (b"\x00\x00\x00\x00\x00\x00\xaa\xaa", None) == parse_tidrange(
        "000000000000aaaa.."
    )

    assert (None, b"\x00\x00\x00\x00\x00\x00\xbb\xbb") == parse_tidrange(
        "..000000000000bbbb"
    )

    assert (None, None) == parse_tidrange("..")

    with raises(TidRangeInvalid) as exc:
        parse_tidrange("invalid")
    assert exc.value.args == ("invalid",)


def test_tidrange_date(europe_paris_timezone):
    # dates in UTC
    assert (
        b"\x03\xc4\x85v\x00\x00\x00\x00",
        b"\x03\xc4\x88\xa0\x00\x00\x00\x00",
    ) == parse_tidrange("2018-01-01 10:30:00 UTC..2018-01-02 UTC")

    # these TIDs are ZODB.TimeStamp.TimeStamp
    assert (TimeStamp(2018, 1, 1, 10, 30, 0).raw(), None) == parse_tidrange(
        "2018-01-01 10:30:00 UTC.."
    )

    # dates in local timezone
    assert (
        b"\x03\xc4\x85:\x00\x00\x00\x00",
        b"\x03\xc4\x88d\x00\x00\x00\x00",
    ) == parse_tidrange("2018-01-01 10:30:00..2018-01-02")

    # dates in natural language (also in local timezone)
    assert (
        b"\x03\xc4\x85:\x00\x00\x00\x00",
        b"\x03\xc4\x88d\x00\x00\x00\x00",
    ) == parse_tidrange("le 1er janvier 2018 à 10h30..2018年1月2日")

    # or relative dates
    assert (None, None) != parse_tidrange("1 month ago..yesterday")
