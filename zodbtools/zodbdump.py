# Copyright (C) 2016-2017  Nexedi SA and Contributors.
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
"""Zodbdump - Tool to dump content of a ZODB database

This program dumps content of a ZODB database.
It uses ZODB Storage iteration API to get list of transactions and for every
transaction prints transaction's header and information about changed objects.

The information dumped is complete raw information as stored in ZODB storage
and should be suitable for restoring the database from the dump file bit-to-bit
identical to its original(*). It is dumped in semi text-binary format where
object data is output as raw binary and everything else is text.

There is also shortened mode activated via --hashonly where only hash of object
data is printed without content.

Dump format:

    txn <tid> <status|quote>
    user <user|quote>
    description <description|quote>
    extension <raw_extension|quote>
    obj <oid> (delete | from <tid> | <size> <hashfunc>:<hash> (-|LF <raw-content>)) LF
    obj ...
    ...
    obj ...
    LF
    txn ...

quote:      quote string with " with non-printable and control characters \-escaped
hashfunc:   one of sha1, sha256, sha512 ...

(*) It is possible to obtain transaction metadata in raw form only in recent ZODB.
    See https://github.com/zopefoundation/ZODB/pull/183 for details.

TODO also protect txn record by hash.
"""

from __future__ import print_function
from zodbtools.util import ashex, sha1, txnobjv, parse_tidrange, TidRangeInvalid,   \
        storageFromURL
from ZODB._compat import loads, _protocol, BytesIO
from zodbpickle.slowpickle import Pickler as pyPickler
#import pickletools

import sys
import logging
from golang.gcompat import qq

# txn_raw_extension returns raw extension from txn metadata
def txn_raw_extension(stor, txn):
    # if txn provides ZODB.interfaces.IStorageTransactionInformationRaw - use it directly
    raw_extension = getattr(txn, "extension_bytes", None)
    if raw_extension is not None:
        return raw_extension

    # otherwise do best effort to generate raw_extension from txn.extension
    # in a rational way
    stor_name = "(%s, %s)" % (type(stor).__name__, stor.getName())
    if stor_name not in _already_warned_notxnraw:
        logging.warn("%s: storage does not provide IStorageTransactionInformationRaw ...", stor_name)
        logging.warn("... will do best-effort to dump pickles in stable order but this cannot be done 100% correctly")
        logging.warn("... please upgrade your ZODB & storage: see https://github.com/zopefoundation/ZODB/pull/183 for details.")
        _already_warned_notxnraw.add(stor_name)

    return serializeext(txn.extension)

# set of storage names already warned for not providing IStorageTransactionInformationRaw
_already_warned_notxnraw = set()

# zodbdump dumps content of a ZODB storage to a file.
# please see module doc-string for dump format and details
def zodbdump(stor, tidmin, tidmax, hashonly=False, out=sys.stdout):
    first = True

    for txn in stor.iterator(tidmin, tidmax):
        vskip = "\n"
        if first:
            vskip = ""
            first = False

        # XXX .status not covered by IStorageTransactionInformation
        # XXX but covered by BaseStorage.TransactionRecord
        out.write("%stxn %s %s\nuser %s\ndescription %s\nextension %s\n" % (
            vskip, ashex(txn.tid), qq(txn.status),
            qq(txn.user),
            qq(txn.description),
            qq(txn_raw_extension(stor, txn)) ))

        objv = txnobjv(txn)

        for obj in objv:
            entry = "obj %s " % ashex(obj.oid)
            write_data = False

            if obj.data is None:
                entry += "delete"

            # was undo and data taken from obj.data_txn
            elif obj.data_txn is not None:
                entry += "from %s" % ashex(obj.data_txn)

            else:
                # XXX sha1 is hardcoded for now. Dump format allows other hashes.
                entry += "%i sha1:%s" % (len(obj.data), ashex(sha1(obj.data)))
                write_data = True

            out.write(entry)

            if write_data:
                if hashonly:
                    out.write(" -")
                else:
                    out.write("\n")
                    out.write(obj.data)

            out.write("\n")

# ----------------------------------------
# XPickler is Pickler that tries to save objects stably
# in other words dicts/sets/... are pickled with items emitted always in the same order.
#
# NOTE we order objects by regular python objects "<", and in general case
# python fallbacks to comparing objects by their addresses, so comparision
# result is not in general stable from run to run. The following program
# prints True/False randomly with p. 50%:
# ---- 8< ----
# from random import choice
# class A: pass
# class B: pass
# if choice([True, False]):
#     a = A()
#     b = B()
# else:
#     b = B()
#     a = A()
# print a < b
# ---- 8< ----
#
# ( related reference: https://pythonhosted.org/BTrees/#total-ordering-and-persistence )
#
# We are ok with this semi-working solution(*) because it is only a fallback:
# for proper zodbdump usage it is adviced for storage to provide
# IStorageTransactionInformationRaw with all raw metadata directly accessible.
#
# (*) but 100% working e.g. for keys = only strings or integers
#
# NOTE cannot use C pickler because hooking into internal machinery is not possible there.
class XPickler(pyPickler):

    dispatch = pyPickler.dispatch.copy()

    def save_dict(self, obj):
        # original pickler emits items taken from obj.iteritems()
        # let's prepare something with .iteritems() but emits those objs items ordered

        items = obj.items()
        items.sort()   # sorts by key
        xitems = asiteritems(items)

        super(self, XPickler).save_dict(xitems)

    def save_set(self, obj):
        # set's reduce always return 3 values
        # https://github.com/python/cpython/blob/309fb90f/Objects/setobject.c#L1954
        typ, keyv, dict_ = obj.__reduce_ex__(self.proto)
        keyv.sort()

        rv = (typ, keyv, dict_)
        self.save_reduce(obj=obj, *rv)

    dispatch[set] = save_set

# asiteritems creates object that emits prepared items via .iteritems()
# see save_dict() above for why/where it is needed.
class asiteritems(object):

    def __init__(self, items):
        self._items = items

    def iteritems(self):
        return iter(self._items)


# serializeext canonically serializes transaction's metadata "extension" dict
def serializeext(ext):
    # ZODB iteration API gives us depickled extensions and only that.
    # So for dumping in raw form we need to pickle it back hopefully getting
    # something close to original raw data.

    if not ext:
        # ZODB usually does this: encode {} as empty "", not as "}."
        # https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/BaseStorage.py#L194
        #
        # and here are decoders:
        # https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/FileStorage/FileStorage.py#L1145
        # https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/FileStorage/FileStorage.py#L1990
        # https://github.com/zopefoundation/ZODB/blob/2490ae09/src/ZODB/fstools.py#L66
        # ...
        return b""

    buf = BytesIO()
    p = XPickler(buf, _protocol)
    p.dump(ext)
    out = buf.getvalue()
    #out = pickletools.optimize(out) # remove unneeded PUT opcodes
    assert loads(out) == ext
    return out

# ----------------------------------------
import sys, getopt

summary = "dump content of a ZODB database"

def usage(out):
    print("""\
Usage: zodb dump [OPTIONS] <storage> [tidmin..tidmax]
Dump content of a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

Options:

        --hashonly  dump only hashes of objects without content
    -h  --help      show this help
""", file=out)

def main(argv):
    hashonly = False

    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help", "hashonly"])
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
        storurl = argv[0]
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

    stor = storageFromURL(storurl, read_only=True)

    zodbdump(stor, tidmin, tidmax, hashonly)
