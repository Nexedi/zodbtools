# -*- coding: utf-8 -*-
# Copyright (C) 2024-  Nexedi SA and Contributors.
#                          Jérome Perrin <jerome@nexedi.com>
#                          Kirill Smelkov <kirr@nexedi.com>
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
"""zodbcatobj - Print an object stored in ZODB database from its xid"""

from __future__ import print_function
import sys

from golang import func, defer
import ZODB.POSException
from ZODB.BaseStorage import DataRecord
from zodbtools import zodbdump
from zodbtools.util import asbinstream, ashex, loadAt, parse_xid, prettyPrintRegistry, sha1, storageFromURL, XidInvalid


def zodbcatobj(stor, xid, pretty='raw', out=asbinstream(sys.stdout)):
    data, _ = loadAt(stor, xid.oid, xid.tid)
    data = prettyPrintRegistry[pretty].format_record(data)
    out.write(data)


def zodbdumpobj(stor, xid, hashonly=False, pretty='raw', out=asbinstream(sys.stdout)):
    data, _ = loadAt(stor, xid.oid, xid.tid)
    obj = DataRecord(
                oid  = xid.oid,
                tid  = xid.tid,
                data = data,
                prev = None)  # no copy detection at catobj - just dump raw content
    zodbdump._dumpobj(obj, hashonly, pretty, out)


# ----------------------------------------
import getopt

summary = "print a record stored in a ZODB database"

def usage(out):
    print("""\
Usage: zodb catobj [OPTIONS] <storage> <xid>
Print a record stored in a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.
<xid> is object address (see 'zodb help xid').

Options:
        --pretty=<format> output in a given format (see 'zodb help pretty')
        --hashonly        dump only hashes of record without content
        --raw             dump object data without any headers. Only one object allowed.
    -h  --help            show this help

""", file=out)


@func
def main(argv):
    raw      = False
    hashonly = False
    pretty   = 'raw'

    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help", "hashonly", "pretty=", "raw"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, arg in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        if opt in ("--hashonly",):
            hashonly = True
        if opt in ("--pretty",):
            pretty = arg
            if pretty not in prettyPrintRegistry:
                print("E: unsupported pretty format: %s" % pretty, file=sys.stderr)
                sys.exit(2)
        if opt in ("--raw",):
            raw = True

    try:
        storurl, xid = argv
    except ValueError:
        usage(sys.stderr)
        sys.exit(2)

    try:
        xid = parse_xid(xid)
    except XidInvalid:
        print("E: invalid object address: %s" % xid, file=sys.stderr)
        sys.exit(2)

    if raw and hashonly:
        print("E: --hashonly & --raw are incompatible")

    stor = storageFromURL(storurl, read_only=True)
    defer(stor.close)

    try:
        if raw:
            zodbcatobj(stor, xid, pretty)
        else:
            zodbdumpobj(stor, xid, hashonly, pretty)
    except ZODB.POSException.POSKeyError as e:
        print("E: object address not found in database: %r" % e, file=sys.stderr)
        sys.exit(2)
