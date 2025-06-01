# -*- coding: utf-8 -*-
# Copyright (C) 2025 Nexedi SA and Contributors.
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

import os.path
from glob import glob
from io import BytesIO
import subprocess
import sys

import pytest
from golang import defer, func

from ZODB.FileStorage import FileStorage
from ZODB.DB import DB

from zodbtools.util import parse_xid, storageFromURL
from zodbtools.zodbcatobj import zodbcatobj
from zodbtools.test.testutil import fs1_testdata_py23, normalize_zpickledis


@func
def test_zodbcatobj_pretty(tmpdir, ztestdata):
    zcatoobj_oks = list(glob(os.path.join(ztestdata.prefix, 'zcatobj.*.ok')))
    assert zcatoobj_oks
    for path in zcatoobj_oks:
        _, tid, oid, pretty, _ = os.path.basename(path).split('.')
        with open(path, 'rb') as f:
            dumpok = f.read()
        if pretty == 'zpickledis':
            dumpok = normalize_zpickledis(dumpok)
        tfs1 = fs1_testdata_py23(tmpdir, '%s/data.fs' % ztestdata.prefix)
        stor = FileStorage(tfs1, read_only=True)
        defer(stor.close)
        out = BytesIO()
        zodbcatobj(stor, parse_xid('%s:%s' % (tid, oid)), hashonly=False, pretty=pretty, out=out)
        assert out.getvalue() == dumpok


@pytest.fixture
def stor_xid_hash(tmpdir, ztestdata):
    hashes = {
        'py2_pickle1': b'sha1:efa4af8429cb9e35cbe6509ae15f6b6741557880',
        'py2_pickle2': b'sha1:e094ea6598a1ef872b8b068bd09291a5a9162a95',
        'py2_pickle3': b'sha1:d2a3de492eb622b9f17f8f25200723aec344352f',
        'py3_pickle3': b'sha1:1c003accb6db6ef37e4f00c7e7387ef75c4bdf30',
    }
    tfs1 = fs1_testdata_py23(tmpdir, '%s/data.fs' % ztestdata.prefix)
    stor = FileStorage(tfs1, read_only=True)
    try:
        yield (
            stor,
            parse_xid("0x0285cbae66d3a14c:0x000000000000000b"),
            hashes[ztestdata.prefix.split('/')[-1]],
        )
    finally:
        stor.close()

def test_zodbcatobj_hash(stor_xid_hash):
    stor, xid, hash_ = stor_xid_hash
    out = BytesIO()
    zodbcatobj(stor, xid, hashonly=True, pretty=False, out=out)
    assert out.getvalue() == hash_


def test_zodbcatobj_cmd(zsrv):
    stor = storageFromURL(zsrv.zurl)
    DB(stor).close()
    stor.close()
    
    assert b'PersistentMapping' in subprocess.check_output(
        [
            sys.executable,
            '-m',
            'zodbtools.zodb',
            'catobj',
            '--pretty=zpickledis',
            zsrv.zurl,
            'tomorrow:0x0000000000000000',
        ],
    )

    p = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'zodbtools.zodb',
            'catobj',
            '--pretty=zpickledis',
            zsrv.zurl,
            'yesterday:0x0000000000012345',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    _, stderr = p.communicate()
    assert p.returncode == 2
    assert "E: object address not found in database" in stderr

