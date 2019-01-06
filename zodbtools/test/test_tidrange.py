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

from pytest import raises

from zodbtools.util import parse_tidrange, TidRangeInvalid


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
