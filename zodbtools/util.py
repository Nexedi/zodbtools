# zodbtools - various utility routines
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

import hashlib, struct, codecs
import zodburi
from six.moves.urllib_parse import urlsplit, urlunsplit
from zlib import crc32, adler32

def ashex(s):
    return s.encode('hex')

def fromhex(s):
    return codecs.decode(s, 'hex')

def sha1(data):
    m = hashlib.sha1()
    m.update(data)
    return m.digest()

# something that is greater than everything else
class Inf:
    def __cmp__(self, other):
        return +1
inf = Inf()

# get next item from iter -> (item, !stop)
def nextitem(it):
    try:
        item = it.next()
    except StopIteration:
        return None, False
    else:
        return item, True

# objects of a IStorageTransactionInformation
def txnobjv(txn):
    objv = []
    for obj in txn:
        assert obj.tid == txn.tid
        assert obj.version == ''
        objv.append(obj)

    objv.sort(key = lambda obj: obj.oid)    # in canonical order
    return objv

# "tidmin..tidmax" -> (tidmin, tidmax)
class TidRangeInvalid(Exception):
    pass

def parse_tidrange(tidrange):
    try:
        tidmin, tidmax = tidrange.split("..")
    except ValueError:  # not exactly 2 parts in between ".."
        raise TidRangeInvalid(tidrange)

    try:
        tidmin = tidmin.decode("hex")
        tidmax = tidmax.decode("hex")
    except TypeError:   # hex decoding error
        raise TidRangeInvalid(tidrange)

    # empty tid means -inf / +inf respectively
    # ( which is None in IStorage.iterator() )
    return (tidmin or None, tidmax or None)


# storageFromURL opens a ZODB-storage specified by url
# read_only specifies read or read/write mode for requested access:
# - None: use default mode specified by url
# - True/False: explicitly request read-only / read-write mode
def storageFromURL(url, read_only=None):
    # no schema -> file://
    if "://" not in url:
        url = "file://" + url

    # read_only -> url
    if read_only is not None:
        scheme, netloc, path, query, fragment = urlsplit(url)
        # XXX this won't have effect with zconfig:// but for file:// neo://
        #     zeo:// etc ... it works
        if scheme != "zconfig":
            if len(query) > 0:
                query += "&"
            query += "read_only=%s" % read_only
            url = urlunsplit((scheme, netloc, path, query, fragment))

    stor_factory, dbkw = zodburi.resolve_uri(url)
    stor = stor_factory()

    return stor

# ---- hashing ----

# hasher that discards data
class NullHasher:
    name = "null"
    digest_size = 1

    def update(self, data):
        pass

    def digest(self):
        return b'\0'

    def hexdigest(self):
        return "00"

# adler32 in hashlib interface
class Adler32Hasher:
    name = "adler32"
    digest_size = 4

    def __init__(self):
        self._h = adler32('')

    def update(self, data):
        self._h = adler32(data, self._h)

    def digest(self):
        return struct.pack('>I', self._h & 0xffffffff)

    def hexdigest(self):
        return '%08x' % (self._h & 0xffffffff)

# crc32 in hashlib interface
class CRC32Hasher:
    name = "crc32"
    digest_size = 4

    def __init__(self):
        self._h = crc32('')

    def update(self, data):
        self._h = crc32(data, self._h)

    def digest(self):
        return struct.pack('>I', self._h & 0xffffffff)

    def hexdigest(self):
        return '%08x' % (self._h & 0xffffffff)

# {} name -> hasher
hashRegistry = {
    "null":     NullHasher,
    "adler32":  Adler32Hasher,
    "crc32":    CRC32Hasher,
    "sha1":     hashlib.sha1,
    "sha256":   hashlib.sha256,
    "sha512":   hashlib.sha512,
}
