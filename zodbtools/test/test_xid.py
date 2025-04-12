# -*- coding: utf-8 -*-
# Copyright (C) 2019-2025 Nexedi SA and Contributors.
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


from zodbtools.util import Xid, parse_xid, XidInvalid


@pytest.mark.parametrize(
    "xid,tid,oid",
    [
        (
            # hex
            "0285cbac258bf266:0000000000000001",
            b"\x02\x85\xcb\xac\x25\x8b\xf2f",
            b"\x00\x00\x00\x00\x00\x00\x00\x01",
        ),
        (
            # ZODB's tid_repr : oid_repr
            "0x03ff889405753100:0x0144966f",
            b"\x03\xff\x88\x94\x05u1\x00",
            b"\x00\x00\x00\x00\x01D\x96o",
        ),
        (
            # pickletools.dis' unicode representation for oid
            r"0x03ff889405753100:\x00\x00\x00\x00\x00\x01#E",
            b"\x03\xff\x88\x94\x05u1\x00",
            b"\x00\x00\x00\x00\x00\x01#E",
        ),
        (
            # dates supported by tidrange
            "2018-01-01T10:30:00Z:0000000000000001",
            b"\x03\xc4\x85v\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x01",
        ),
    ],
)
def test_parse_xid(xid, tid, oid):
    _xid = parse_xid(xid)
    assert isinstance(_xid, Xid)
    assert _xid == (tid, oid)


def test_parse_xid_invalid():
    with pytest.raises(XidInvalid) as exc:
        parse_xid("invalid")
    assert exc.value.args == ("invalid",)

    with pytest.raises(XidInvalid) as exc:
        parse_xid("")
    assert exc.value.args == ("",)

    with pytest.raises(XidInvalid) as exc:
        parse_xid("0285cbac258bf266:invalid")
    assert exc.value.args == ("0285cbac258bf266:invalid",)


def test_xid():
    xid = Xid(b"\x02\x85\xcb\xac\x25\x8b\xf2f", b"\x00\x00\x00\x00\x00\x00\x00\x01")
    assert xid.tid == b"\x02\x85\xcb\xac\x25\x8b\xf2f"
    assert xid.oid == b"\x00\x00\x00\x00\x00\x00\x00\x01"

    assert xid.next_tid == b"\x02\x85\xcb\xac\x25\x8b\xf2g"

    assert repr(xid) == "Xid('0x0285cbac258bf266:0x01')"
    assert str(xid) == '0x0285cbac258bf266:0x01'
