#!/usr/bin/env python
# Copyright (C) 2016  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
"""Zodbdump - Tool to dump content of a ZODB database

TODO format     (WARNING dump format is not yet stable)

txn <tid> (<status>)
user <user|encode?>
description <description|encode?>
extension <extension|encode?>
obj <oid> (delete | from <tid> | <sha1> <size> (LF <content>)?) LF     XXX do we really need back <tid>
---- // ----
LF
txn ...

"""

from __future__ import print_function
from zodbtool.util import ashex, sha1, txnobjv, parse_tidrange, TidRangeInvalid


def zodbdump(stor, tidmin, tidmax, hashonly=False):
    first = True

    for txn in stor.iterator(tidmin, tidmax):
        if not first:
            print()
        first = False

        print('txn %s (%s)' % (ashex(txn.tid), txn.status))
        print('user: %r' % (txn.user,))                    # XXX encode
        print('description:', txn.description)      # XXX encode
        print('extension:', txn.extension)          # XXX dict, encode

        objv = txnobjv(txn)

        for obj in objv:
            entry = 'obj %s ' % ashex(obj.oid)
            if obj.data is None:
                entry += 'delete'

            # was undo and data taken from obj.data_txn
            elif obj.data_txn is not None:
                entry += 'from %s' % ashex(obj.data_txn)

            else:
                entry += '%s %i' % (ashex(sha1(obj.data)), len(obj.data))
                if not hashonly:
                    entry += '\n'
                    entry += obj.data

            print(entry)


# ----------------------------------------
import ZODB.config
import sys, getopt

def usage(out):
    print("""\
Usage: zodbdump [OPTIONS] <storage> [tidmin..tidmax]
Dump content of a ZODB database.

<storage> is a file with ZConfig-based storage definition, e.g.

    %import neo.client
    <NEOStorage>
        master_nodes    ...
        name            ...
    </NEOStorage>

Options:

        --hashonly  dump only hashes of objects without content
    -h  --help      show this help
""", file=out)

def main():
    hashonly = False

    try:
        optv, argv = getopt.getopt(sys.argv[1:], "h", ["help", "hashonly"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        if opt in ("--hashonly"):
            hashonly = True

    try:
        storconf = argv[0]
    except IndexError:
        usage(sys.stderr)
        sys.exit(2)

    # parse tidmin..tidmax
    tidmin = tidmax = None
    if len(argv) > 1:
        try:
            tidmin, tidmax = parse_tidrange(argv[1])
        except TidRangeInvalid as e:
            print("E: invalid tidrange: %s" % e, file=sys.stderr)
            sys.exit(2)

    stor = ZODB.config.storageFromFile(open(storconf, 'r'))

    zodbdump(stor, tidmin, tidmax, hashonly)

if __name__ == '__main__':
    main()
