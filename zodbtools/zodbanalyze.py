# -*- coding: utf-8 -*-
# Copyright (C) 2002-2017 Zope Foundation + Nexedi + Contributors
# See LICENSE-ZPL.txt for full licensing terms.

# Based on a transaction analyzer by Matt Kromer.

from __future__ import print_function

import sys
import os
import getopt
from six.moves import dbm_gnu as dbm
import tempfile
import shutil
from ZODB.FileStorage import FileIterator, packed_version
from ZODB.FileStorage.format import FileStorageFormatter
from ZODB.utils import get_pickle_metadata
from zodbtools.util import storageFromURL, parse_tidrange, ashex
from golang import func, defer, b

class DeltaFileStorage(
    FileStorageFormatter,
    ):
    def __init__(self, file_name, **kw):
        self._file_name = file_name

    def iterator(self, start=None, stop=None):
        return DeltaFileIterator(self._file_name, start, stop)

    def close(self):
        pass

class DeltaFileIterator(FileIterator):
    def __init__(self, filename, start=None, stop=None, pos=0):
        assert isinstance(filename, str)
        file = open(filename, 'rb')
        self._file = file
        file.seek(0,2)
        self._file_size = file.tell()
        if pos > self._file_size:
            raise ValueError("Given position is greater than the file size",
                             pos, self._file_size)
        self._pos = pos
        assert start is None or isinstance(start, str)
        assert stop is None or isinstance(stop, str)
        self._start = start
        self._stop = stop
        if start:
            if self._file_size <= 4:
                return
            self._skip_to_start(start)

class Report:
    def __init__(self, use_dbm=False, delta_fs=False):
        self.use_dbm = use_dbm
        self.delta_fs = delta_fs
        if use_dbm:
            self.temp_dir = tempfile.mkdtemp()
            self.OIDMAP = dbm.open(os.path.join(self.temp_dir, 'oidmap.db'),
                                   'nf')
            self.USEDMAP = dbm.open(os.path.join(self.temp_dir, 'usedmap.db'),
                                    'nf')
        else:
            self.OIDMAP = {}
            self.USEDMAP = {}
        self.TYPEMAP = {}
        self.TYPESIZE = {}
        self.TIDS = 0
        self.OIDS = 0
        self.DBYTES = 0
        self.COIDS = 0
        self.CBYTES = 0
        self.FOIDS = 0
        self.FBYTES = 0
        self.COIDSMAP = {}
        self.CBYTESMAP = {}
        self.FOIDSMAP = {}
        self.FBYTESMAP = {}
        self.tidmin = None  # first scanned transaction
        self.tidmax = None  # last  ----//----

def shorten(s, n):
    l = len(s)
    if l <= n:
        return s
    while len(s) + 3 > n: # account for ...
        i = s.find(".")
        if i == -1:
            # In the worst case, just return the rightmost n bytes
            return s[-n:]
        else:
            s = s[i + 1:]
            l = len(s)
    return "..." + s

def report(rep, csv=False):
    delta_fs = rep.delta_fs
    if not csv:
        if rep.TIDS == 0:
            print ("# ø")
            print ("No transactions processed")
            return

        print ("# %s..%s" % (ashex(rep.tidmin), ashex(rep.tidmax)))
        print ("Processed %d records in %d transactions" % (rep.OIDS, rep.TIDS))
        print ("Average record size is %7.2f bytes" % (rep.DBYTES * 1.0 / rep.OIDS))
        print ("Average transaction size is %7.2f bytes" %
               (rep.DBYTES * 1.0 / rep.TIDS))

        print ("Types used:")
    if delta_fs:
        if csv:
            fmt = "%s,%s,%s,%s,%s"
            fmtp = "%s,%d,%d,%f%%,%f" # per-class format
        else:
            fmt = "%-46s %7s %9s %6s %7s"
            fmtp = "%-46s %7d %9d %5.1f%% %7.2f" # per-class format
        print (fmt % ("Class Name", "T.Count", "T.Bytes", "Pct", "AvgSize"))
        if not csv:
            print (fmt % ('-'*46, '-'*7, '-'*9, '-'*5, '-'*7))
    else:
        if csv:
            fmt = "%s,%s,%s,%s,%s,%s,%s,%s,%s"
            fmtp = "%s,%d,%d,%f%%,%f,%d,%d,%d,%d" # per-class format
        else:
            fmt = "%-46s %7s %9s %6s %7s %7s %9s %7s %9s"
            fmtp = "%-46s %7d %9d %5.1f%% %7.2f %7d %9d %7d %9d" # per-class format
        print (fmt % ("Class Name", "T.Count", "T.Bytes", "Pct", "AvgSize",
                      "C.Count", "C.Bytes", "O.Count", "O.Bytes"))
        if not csv:
            print (fmt % ('-'*46, '-'*7, '-'*9, '-'*5, '-'*7, '-'*7, '-'*9, '-'*7, '-'*9))
    fmts = "%46s %7d %8dk %5.1f%% %7.2f" # summary format
    cumpct = 0.0
    for t in sorted(rep.TYPEMAP.keys(), key=lambda a:rep.TYPESIZE[a]):
        pct = rep.TYPESIZE[t] * 100.0 / rep.DBYTES
        cumpct += pct
        if csv:
            t_display = t
        else:
            t_display = shorten(t, 46)
        if delta_fs:
            print (fmtp % (t_display, rep.TYPEMAP[t], rep.TYPESIZE[t],
                           pct, rep.TYPESIZE[t] * 1.0 / rep.TYPEMAP[t]))
        else:
            print (fmtp % (t_display, rep.TYPEMAP[t], rep.TYPESIZE[t],
                           pct, rep.TYPESIZE[t] * 1.0 / rep.TYPEMAP[t],
                           rep.COIDSMAP[t], rep.CBYTESMAP[t],
                           rep.FOIDSMAP.get(t, 0), rep.FBYTESMAP.get(t, 0)))

    if csv:
        return

    if delta_fs:
        print (fmt % ('='*46, '='*7, '='*9, '='*5, '='*7))
        print ("%46s %7d %9s %6s %6.2f" % ('Total Transactions', rep.TIDS, ' ',
                                          ' ', rep.DBYTES * 1.0 / rep.TIDS))
        print (fmts % ('Total Records', rep.OIDS, rep.DBYTES, cumpct,
                       rep.DBYTES * 1.0 / rep.OIDS))
    else:
        print (fmt % ('='*46, '='*7, '='*9, '='*5, '='*7, '='*7, '='*9, '='*7, '='*9))
        print ("%46s %7d %9s %6s %6.2fk" % ('Total Transactions', rep.TIDS, ' ',
            ' ', rep.DBYTES * 1.0 / rep.TIDS / 1024.0))
        print (fmts % ('Total Records', rep.OIDS, rep.DBYTES / 1024.0, cumpct,
                       rep.DBYTES * 1.0 / rep.OIDS))

        print (fmts % ('Current Objects', rep.COIDS, rep.CBYTES / 1024.0,
                       rep.CBYTES * 100.0 / rep.DBYTES,
                       rep.CBYTES * 1.0 / rep.COIDS))
        if rep.FOIDS:
            print (fmts % ('Old Objects', rep.FOIDS, rep.FBYTES / 1024.0,
                           rep.FBYTES * 100.0 / rep.DBYTES,
                           rep.FBYTES * 1.0 / rep.FOIDS))

@func
def analyze(path, use_dbm, delta_fs, tidmin, tidmax):
    if delta_fs:
        fs = DeltaFileStorage(path, read_only=1)
    else:
        fs = storageFromURL(path, read_only=1)
    defer(fs.close)
    fsi = fs.iterator(tidmin, tidmax)
    report = Report(use_dbm, delta_fs)
    for txn in fsi:
        analyze_trans(report, txn)
    if use_dbm:
        shutil.rmtree(report.temp_dir)
    return report

def analyze_trans(report, txn):
    report.TIDS += 1
    if report.tidmin is None:
        # first seen transaction
        report.tidmin = txn.tid
    report.tidmax = txn.tid
    for rec in txn:
        analyze_rec(report, rec)

def get_type(record):
    mod, klass = get_pickle_metadata(record.data)
    return "%s.%s" % (mod, klass)

def analyze_rec(report, record):
    oid = record.oid
    report.OIDS += 1
    if record.data is None:
        # No pickle -- aborted version or undo of object creation.
        return
    try:
        size = len(record.data) # Ignores various overhead
        report.DBYTES += size
        if report.delta_fs:
            type = get_type(record)
            report.TYPEMAP[type] = report.TYPEMAP.get(type, 0) + 1
            report.TYPESIZE[type] = report.TYPESIZE.get(type, 0) + size
        else:
            if oid not in report.OIDMAP:
                type = get_type(record)
                report.OIDMAP[oid] = type
                if report.use_dbm:
                    report.USEDMAP[oid] = str(size)
                else:
                    report.USEDMAP[oid] = size
                report.COIDS += 1
                report.CBYTES += size
                report.COIDSMAP[type] = report.COIDSMAP.get(type, 0) + 1
                report.CBYTESMAP[type] = report.CBYTESMAP.get(type, 0) + size
            else:
                type = b(report.OIDMAP[oid])
                if report.use_dbm:
                    fsize = int(report.USEDMAP[oid])
                    report.USEDMAP[oid] = str(size)
                else:
                    fsize = report.USEDMAP[oid]
                    report.USEDMAP[oid] = size
                report.FOIDS += 1
                report.FBYTES += fsize
                report.CBYTES += size - fsize
                report.FOIDSMAP[type] = report.FOIDSMAP.get(type, 0) + 1
                report.FBYTESMAP[type] = report.FBYTESMAP.get(type, 0) + fsize
                report.CBYTESMAP[type] = report.CBYTESMAP.get(type, 0) + size - fsize
            report.TYPEMAP[type] = report.TYPEMAP.get(type, 0) + 1
            report.TYPESIZE[type] = report.TYPESIZE.get(type, 0) + size
    except Exception as err:
        print (err, file=sys.stderr)

__doc__ = """%(program)s: Analyzer for ZODB data or repozo deltafs

usage: %(program)s [options] <storage> [<tidrange>]

<storage> is an URL (see 'zodb help zurl') or /path/to/file.deltafs(*)
<tidrange> is a history range (see 'zodb help tidrange') to analyze.

Options:
  -h, --help                 this help screen
  -c, --csv                  output CSV
  -d, --dbm                  use DBM as temporary storage to limit memory usage
                             (no meaning for deltafs case)
(*) Note:
  Input deltafs file should be uncompressed.
"""

summary = "analyze ZODB database or repozo deltafs usage"

def usage(stream, msg=None):
    if msg:
        print (msg, file=stream)
        print (file=stream)
    print (__doc__ % {"program": "zodb analyze"}, file=stream)


def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:],
                                   'hcd', ['help', 'csv', 'dbm'])
        path = args[0]
    except (getopt.GetoptError, IndexError) as msg:
        usage(sys.stderr, msg)
        sys.exit(2)

    # parse tidmin..tidmax
    tidmin = tidmax = None
    if len(args) > 1:
        tidmin, tidmax = parse_tidrange(args[1])

    csv = False
    use_dbm = False
    for opt, args in opts:
        if opt in ('-c', '--csv'):
            csv = True
        if opt in ('-d', '--dbm'):
            use_dbm = True
        if opt in ('-h', '--help'):
            usage(sys.stdout)
            sys.exit()
    # try to see whether it is zurl or a path to file.deltafs
    delta_fs = False
    if os.path.exists(path):
        header = open(path, 'rb').read(4)
        if header != packed_version:
            delta_fs = True
            _orig_read_data_header = FileStorageFormatter._read_data_header
            def _read_data_header(self, pos, oid=None):
                h = _orig_read_data_header(self, pos, oid=oid)
                h.tloc = self._tpos
                return h
            FileStorageFormatter._read_data_header = _read_data_header
    report(analyze(path, use_dbm, delta_fs, tidmin, tidmax), csv)
