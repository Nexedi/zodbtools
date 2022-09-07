# -*- coding: utf-8 -*-
# zodbtools - various utility routines
# Copyright (C) 2016-2022  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#                          Jérome Perrin <jerome@nexedi.com>
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

import hashlib, struct, codecs, io
import zodburi
from six.moves.urllib_parse import urlsplit, urlunsplit
from zlib import crc32, adler32
from ZODB.TimeStamp import TimeStamp
import dateparser

from golang import b


def ashex(s):
    # type: (bytes) -> bstr
    return b(codecs.encode(s, 'hex'))

def fromhex(s):
    # type: (Union[str,bytes]) -> bytes
    return codecs.decode(s, 'hex')

def sha1(data):
    # type: (bytes) -> bytes
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
class TidInvalid(ValueError):
    pass


class TidRangeInvalid(ValueError):
    pass


def parse_tid(tid_string, raw_only=False):
    """Try to parse `tid_string` as a time and returns the
    corresponding raw TID.
    If `tid_string` cannot be parsed as a time, assume it was
    already a TID.
    This function also raise TidRangeInvalid when `tid_string`
    is invalid.
    """
    assert isinstance(tid_string, (str, bytes))

    # If it "looks like a TID", don't try to parse it as time,
    # because parsing is slow.
    if len(tid_string) == 16:
        try:
            return fromhex(tid_string)
        except ValueError:
            pass

    if raw_only:
        # either it was not 16-char string or hex decoding failed
        raise TidInvalid(tid_string)

    # preprocess to support `1.day.ago` style formats like git log does.
    if "ago" in tid_string:
        tid_string = tid_string.replace(".", " ").replace("_", " ")
    parsed_time = dateparser.parse(
        tid_string,
        settings={
            'TO_TIMEZONE': 'UTC',
            'RETURN_AS_TIMEZONE_AWARE': True
        })

    if not parsed_time:
        # parsing as date failed
        raise TidInvalid(tid_string)

    # build a ZODB.TimeStamp to convert as a TID
    return TimeStamp(
            parsed_time.year,
            parsed_time.month,
            parsed_time.day,
            parsed_time.hour,
            parsed_time.minute,
            parsed_time.second + parsed_time.microsecond / 1000000.).raw()


# parse_tidrange parses a string into (tidmin, tidmax).
#
# see `zodb help tidrange` for accepted tidrange syntax.
def parse_tidrange(tidrange):
    try:
        tidmin, tidmax = tidrange.split("..")
    except ValueError:  # not exactly 2 parts in between ".."
        raise TidRangeInvalid(tidrange)

    if tidmin:
        tidmin = parse_tid(tidmin)
    if tidmax:
        tidmax = parse_tid(tidmax)

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
        self._h = adler32(b'')

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
        self._h = crc32(b'')

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

# ---- IO ----

# asbinstream return binary stream associated with stream.
# For example on py3 sys.stdout is io.TextIO which does not allow to write binary data to it.
def asbinstream(stream):
    # type: (IO) -> BinaryIO
    if isinstance(stream, io.TextIOBase):
        return stream.buffer
    return stream


# readfile reads file at path.
def readfile(path): # -> data(bytes)
    with open(path, 'rb') as _:
        return _.read()

# writefile writes data to file at path.
# if the file existed before its old data is erased.
def writefile(path, data):
    with open(path, 'wb') as _:
        _.write(data)
