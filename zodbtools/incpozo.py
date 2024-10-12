#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2002-2017 Zope Foundation + Nexedi + Contributors
# See LICENSE-ZPL.txt for full licensing terms.

# Based on ZODB's repozo do_recover and main functions.

"""
Incremental repozo restore.

Locates the first incremental to start restoring based on output file size.
Checks the previous chunk to detect mismatched backup & destinations.
Restores increments from that point on, following repozo arguments.
"""
import os
import shutil
import sys
from ZODB.scripts.repozo import NoFiles, checksum, find_files, parseargs, log, concat, RECOVER

def do_inc_recover(options):
    repofiles = find_files(options)
    if not repofiles:
        if options.date:
            raise NoFiles('No files in repository before %s', options.date)
        else:
            raise NoFiles('No files in repository')
    datfile = os.path.splitext(repofiles[0])[0] + '.dat'
    log('Recovering file to %s', options.output)
    with open(datfile) as fp, open(options.output, 'r+b') as outfp:
        outfp.seek(0, 2)
        initial_length = outfp.tell()
        previous_chunk = None
        for line in fp:
            fn, startpos, endpos, _ = chunk = line.split()
            startpos = int(startpos)
            endpos = int(endpos)
            if endpos > initial_length:
                break
            previous_chunk = chunk
        else:
            # XXX: log + return so exit status is zero ?
            raise NoFiles('Target file is longer than or as large at latest backup, doing nothing')
        if previous_chunk is None:
            # XXX: trigger a normal restore ?
            raise NoFiles('Target file shorter than full backup, doing nothing')
        check_start = int(previous_chunk[1])
        check_end = int(previous_chunk[2])
        outfp.seek(check_start, 0)
        if previous_chunk[3] != checksum(outfp, check_end - check_start):
            raise NoFiles('Last whole common chunk checksum did not match with backup, doing nothing')
        assert outfp.tell() == startpos, (outfp.tell(), startpos)
        if startpos < initial_length:
            log('Truncating target file %i bytes before its end', initial_length - startpos)
        filename = os.path.join(options.repository,
                                os.path.basename(fn))
        first_file_to_restore = repofiles.index(filename)
        assert first_file_to_restore > 0, (first_file_to_restore, options.repository, fn, filename, repofiles)
        reposz, reposum = concat(repofiles[first_file_to_restore:], outfp)
    log('Recovered %s bytes, md5: %s', reposz, reposum)

    if options.output is not None:
        last_base = os.path.splitext(repofiles[-1])[0]
        source_index = '%s.index' % last_base
        target_index = '%s.index' % options.output
        if os.path.exists(source_index):
            log('Restoring index file %s to %s', source_index, target_index)
            shutil.copyfile(source_index, target_index)
        else:
            log('No index file to restore: %s', source_index)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    options = parseargs(argv)
    assert options.mode == RECOVER, 'This tool only supports "recover" (-R|--recover) mode'
    assert options.output is not None, 'This tool cannot recover to stdout'
    try:
        do_inc_recover(options)
    except NoFiles as e:
        sys.exit(str(e))

if __name__ == '__main__':
    main()
