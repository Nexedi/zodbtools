# -*- coding: utf-8 -*-
# Copyright (C) 2017-2020  Nexedi SA and Contributors.
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
"""Zodbinfo - Print general information about a ZODB database"""

from __future__ import print_function
from zodbtools.util import ashex, storageFromURL
from collections import OrderedDict
from golang import func, defer
import sys

def _last_tid(stor):
    print("W: last_tid is deprecated alias for head", file=sys.stderr)
    return infoDict["head"](stor)

# {} parameter_name -> get_parameter(stor)
infoDict = OrderedDict([
    ("name", lambda stor: stor.getName()),
    ("size", lambda stor: stor.getSize()),
    ("head", lambda stor: ashex(stor.lastTransaction())),
    ("last_tid", _last_tid),
])

def zodbinfo(stor, parameterv):
    wantnames = False
    if not parameterv:
        parameterv = infoDict.keys()
        wantnames = True

    for parameter in parameterv:
        get_parameter = infoDict.get(parameter)
        if get_parameter is None:
            print("invalid parameter: %s" % parameter, file=sys.stderr)
            sys.exit(1)

        out = ""
        if wantnames:
            out += parameter + "="
        out += "%s" % (get_parameter(stor),)
        print(out)


# ----------------------------------------
import getopt

summary = "print general information about a ZODB database"

def usage(out):
    print("""\
Usage: zodb info [OPTIONS] <storage> [parameter ...]
Print general information about a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

By default info prints information about all storage parameters. If one or
more parameter names are given as arguments, info prints the value of each
named parameter on its own line.

Options:

    -h  --help      show this help
""", file=out)

@func
def main(argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    try:
        storurl = argv[0]
    except IndexError:
        usage(sys.stderr)
        sys.exit(2)

    stor = storageFromURL(storurl, read_only=True)
    defer(stor.close)

    zodbinfo(stor, argv[1:])
