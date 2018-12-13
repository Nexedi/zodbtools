# Copyright (C) 2016-2018  Nexedi SA and Contributors.
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
from zodbtools.util import ashex, fromhex, sha1, txnobjv, parse_tidrange, TidRangeInvalid,   \
        storageFromURL, hashRegistry
from ZODB._compat import loads, _protocol, BytesIO
from zodbpickle.slowpickle import Pickler as pyPickler
#import pickletools
from ZODB.interfaces import IStorageTransactionMetaData
from zope.interface import implementer

import sys
import logging
import re
from golang.gcompat import qq
from golang import strconv

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
    for txn in stor.iterator(tidmin, tidmax):
        # XXX .status not covered by IStorageTransactionInformation
        # XXX but covered by BaseStorage.TransactionRecord
        out.write("txn %s %s\nuser %s\ndescription %s\nextension %s\n" % (
            ashex(txn.tid), qq(txn.status),
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


# ----------------------------------------
# dump reading/parsing

_txn_re = re.compile(b'^txn (?P<tid>[0-9a-f]{16}) "(?P<status>.)"$')
_obj_re = re.compile(b'^obj (?P<oid>[0-9a-f]{16}) ((?P<delete>delete)|from (?P<from>[0-9a-f]{16})|(?P<size>[0-9]+) (?P<hashfunc>\w+):(?P<hash>[0-9a-f]+)(?P<hashonly> -)?)')

# _ioname returns name of the reader r, if it has one.
# if there is no name - '' is returned.
def _ioname(r):
    return getattr(r, 'name', '')


# DumpReader wraps IO reader to read transactions from zodbdump stream.
#
# The reader must provide .readline() and .read() methods.
# The reader must be opened in binary mode.
class DumpReader(object):
    # .lineno   - line number position in read stream

    def __init__(self, r):
        self._r         = r
        self._line      = None  # last read line
        self.lineno     = 0

    def _readline(self):
        l = self._r.readline()
        if l == '':
            self._line = None
            return None # EOF

        l = l.rstrip(b'\n')
        self.lineno += 1
        self._line = l
        return l

    # report a problem found around currently-read line
    def _badline(self, msg):
        raise RuntimeError("%s+%d: invalid line: %s (%r)" % (_ioname(self._r), self.lineno, msg, self._line))

    # readtxn reads one transaction record from input stream and returns
    # Transaction instance or None at EOF.
    def readtxn(self):
        # header
        l = self._readline()
        if l is None:
            return None
        m = _txn_re.match(l)
        if m is None:
            self._badline('no txn start')
        tid = fromhex(m.group('tid'))
        status = m.group('status')

        def get(name):
            l = self._readline()
            if l is None or not l.startswith(b'%s ' % name):
                self._badline('no %s' % name)

            return strconv.unquote(l[len(name) + 1:])

        user          = get(b'user')
        description   = get(b'description')
        extension     = get(b'extension')

        # objects
        objv = []
        while 1:
            l = self._readline()
            if l == '':
                break   # empty line - end of transaction

            if l is None or not l.startswith(b'obj '):
                self._badline('no obj')

            m = _obj_re.match(l)
            if m is None:
                self._badline('invalid obj entry')

            obj = None # will be Object*
            oid = fromhex(m.group('oid'))

            from_ = m.group('from')

            if m.group('delete'):
                obj = ObjectDelete(oid)

            elif from_:
                copy_from = fromhex(from_)
                obj = ObjectCopy(oid, copy_from)

            else:
                size     = int(m.group('size'))
                hashfunc = m.group('hashfunc')
                hashok   = fromhex(m.group('hash'))
                hashonly = m.group('hashonly') is not None
                data     = None # see vvv

                hcls = hashRegistry.get(hashfunc)
                if hcls is None:
                    self._badline('unknown hash function %s' % qq(hashfunc))

                if hashonly:
                    data = HashOnly(size)
                else:
                    # XXX -> io.readfull
                    n = size+1  # data LF
                    data = b''
                    while n > 0:
                        chunk = self._r.read(n)
                        data += chunk
                        n -= len(chunk)
                    self.lineno += data.count('\n')
                    self._line = None
                    if data[-1:] != b'\n':
                        raise RuntimeError('%s+%d: no LF after obj data' % (_ioname(self._r), self.lineno))
                    data = data[:-1]

                    # verify data integrity
                    # TODO option to allow reading corrupted data
                    h = hcls()
                    h.update(data)
                    hash_ = h.digest()
                    if hash_ != hashok:
                        raise RuntimeError('%s+%d: data corrupt: %s = %s, expected %s' % (
                            _ioname(self._r), self.lineno, h.name, ashex(hash_), ashex(hashok)))

                obj = ObjectData(oid, data, hashfunc, hashok)

            objv.append(obj)

        return Transaction(tid, status, user, description, extension, objv)


# Transaction represents one transaction record in zodbdump stream.
@implementer(IStorageTransactionMetaData)
class Transaction(object):
    # .tid              p64         transaction ID
    # .status           char        status of the transaction
    # .user             bytes       transaction author
    # .description      bytes       transaction description
    # .extension_bytes  bytes       transaction extension
    # .objv             []Object*   objects changed by transaction
    def __init__(self, tid, status, user, description, extension, objv):
        self.tid                = tid
        self.status             = status
        self.user               = user
        self.description        = description
        self.extension_bytes    = extension
        self.objv               = objv

    # ZODB wants to work with extension as {} - try to convert it on the fly.
    #
    # The conversion can fail for arbitrary .extension_bytes input.
    # The conversion should become not needed once
    #
    #   https://github.com/zopefoundation/ZODB/pull/183, or
    #   https://github.com/zopefoundation/ZODB/pull/207
    #
    # is in ZODB.
    @property
    def extension(self):
        if not self.extension_bytes:
            return {}
        return loads(self.extension_bytes)

    # zdump returns text representation of a record in zodbdump format.
    def zdump(self):
        z  = 'txn %s %s\n' % (ashex(self.tid), qq(self.status))
        z += 'user %s\n' % qq(self.user)
        z += 'description %s\n' % qq(self.description)
        z += 'extension %s\n' % qq(self.extension_bytes)
        for obj in self.objv:
            z += obj.zdump()
        z += '\n'
        return z


# Object is base class for object records in zodbdump stream.
class Object(object):
    # .oid          p64         object ID
    def __init__(self, oid):
        self.oid = oid

# ObjectDelete represents objects deletion.
class ObjectDelete(Object):

    def __init__(self, oid):
        super(ObjectDelete, self).__init__(oid)

    def zdump(self):
        return 'obj %s delete\n' % (ashex(self.oid))

# ObjectCopy represents object data copy.
class ObjectCopy(Object):
    # .copy_from    tid         copy object data from object's revision tid
    def __init__(self, oid, copy_from):
        super(ObjectCopy, self).__init__(oid)
        self.copy_from = copy_from

    def zdump(self):
        return 'obj %s from %s\n' % (ashex(self.oid), ashex(self.copy_from))

# ObjectData represents record with object data.
class ObjectData(Object):
    # .data         HashOnly | bytes
    # .hashfunc     str             hash function used for integrity
    # .hash_        bytes           hash of the object's data
    def __init__(self, oid, data, hashfunc, hash_):
        super(ObjectData, self).__init__(oid)
        self.data       = data
        self.hashfunc   = hashfunc
        self.hash_      = hash_

    def zdump(self):
        data = self.data
        hashonly = isinstance(data, HashOnly)
        if hashonly:
            size = data.size
        else:
            size = len(data)
        z = 'obj %s %d %s:%s' % (ashex(self.oid), size, self.hashfunc, ashex(self.hash_))
        if hashonly:
            z += ' -'
        else:
            z += '\n'
            z += data
        z += '\n'
        return z

# HashOnly indicated that this ObjectData record contains only hash and does not contain object data.
class HashOnly(object):
    # .size         int
    def __init__(self, size):
        self.size = size

    def __repr__(self):
        return 'HashOnly(%d)' % self.size

    def __eq__(a, b):
        return isinstance(b, HashOnly) and a.size == b.size
