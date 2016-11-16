# zodbtool - various utility routines
# Copyright (C) 2016  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>

import hashlib

def ashex(s):
    return s.encode('hex')

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
