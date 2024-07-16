# Copyright (C) 2019-2024  Nexedi SA and Contributors.
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

import pytest
from zodbtools.test.testutil import zext_supported
import os
from os.path import basename, dirname, relpath
from tempfile import mkdtemp
from shutil import rmtree
import pkg_resources

testdir = dirname(__file__)


# ztestdata is test fixture to run a test wrt particular ZODB testdata case.
#
# It yields all testdata cases generated by gen_testdata.py for both py2 and
# py3 and all covered ZODB pickle kinds.
#
# ztestdata.prefix is where test database and other generated files live.
# ztestdata.prefix + '/data.fs' , in particular, is the path to test database.
@pytest.fixture(params=[
        (name, zext, zkind)
        # NOTE keep in sync with run_with_all_zodb_pickle_kinds
        for name    in ('1',)
        for zext    in (False, True)
        for zkind   in ('py2_pickle1', 'py2_pickle2', 'py2_pickle3', 'py3_pickle3')
    ],
    ids = lambda _: '%s%s/%s' % (_[0], '' if _[1] else '_!zext', _[2]),
)
def ztestdata(request): # -> ZTestData
    name, zext, zkind = request.param
    _ = ZTestData()
    _.name  = name
    _.zext  = zext
    _.zkind = zkind
    return _

class ZTestData(object):
    __slots__ = (
        'name',
        'zext',
        'zkind',
    )

    @property
    def prefix(self):
        _ = '%s/testdata/%s%s/%s' % (testdir, self.name, '' if self.zext else '_!zext', self.zkind)
        return relpath(_)


# zext is a test fixture function object that allows to exercise 2 cases:
#
# - when ZODB does not have txn.extension_bytes support
# - when ZODB might have    txn.extension_bytes support
#
# in a test, zext should be used as follows:
#
#   def test_something(zext):
#       # bytes for an extension dict
#       raw_ext = dumps({...})
#
#       # will be either same as raw_ext, or b'' if ZODB lacks txn.extension_bytes support
#       raw_ext = zext(raw_ext)
#
#       # zext.disabled indicates whether testing for non-empty extension was disabled.
#       if zext.disabled:
#           ...
@pytest.fixture(params=['!zext', 'zext'])
def zext(request):
    if request.param == '!zext':
        # txn.extension_bytes is not working - always test with empty extension
        def _(ext):
            return b''
        _.disabled = True
        return _
    else:
        # txn.extension_bytes might be working - test with given extension and
        # xfail if ZODB does not have necessary support.
        def _(ext):
            return ext
        _.disabled = False
        if not zext_supported():
            request.applymarker(pytest.mark.xfail(reason='ZODB does not have txn.extension_bytes support'))
        return _



# TestZSrv is base class for all test ZODB storages.
class TestZSrv(object):
    # .idname    string to use as subtest ID
    # .zurl      URI to access the storage
    # .all_logs  returns string with all current server logs
    # .teardown  should be called when the server is no longer used
    pass


# TestFileStorage provides FileStorage for tests.
class TestFileStorage:
    idname = 'FileStorage'
    def __init__(self):
        self.tmpd = mkdtemp('', 'test_filestorage.')
        self.zurl = '%s/1.fs' % self.tmpd

    def all_logs(self):
        return "FileStorage: no logs"

    def teardown(self):
        rmtree(self.tmpd)


# TestZEOSrv provides ZEO server for tests.
class TestZEOSrv(TestZSrv):
    idname = 'ZEO'
    def __init__(self):
        from ZEO.tests import forker
        self.zeo_forker = forker

        # .z5 represents whether we are running with ZEO5 or earlier
        dzeo    = pkg_resources.working_set.find(pkg_resources.Requirement.parse('ZEO'))
        v5      = pkg_resources.parse_version('5.0dev')

        assert dzeo is not None
        self.z5 = (dzeo.parsed_version >= v5)

        self.tmpd = mkdtemp('', 'test_zeo.')
        self.log  = '%s/zeo.log' % self.tmpd
        port  = self.zeo_forker.get_port()
        zconf = self.zeo_forker.ZEOConfig(('', port), log=self.log)
        _ = self.zeo_forker.start_zeo_server(path='%s/1.fs' % self.tmpd, zeo_conf=zconf, port=port)
        if self.z5:
            self.addr, self.stop = _
        else:
            self.addr, self.adminaddr, self.pid, self.path = _

        self.zurl = 'zeo://localhost:%d/' % port

    def all_logs(self):
        log = '%s:\n\n' % basename(self.log)
        with open(self.log) as f:
            log += f.read()
        return log

    def teardown(self):
        if self.z5:
            self.stop()
        else:
            self.zeo_forker.shutdown_zeo_server(self.adminaddr)
            os.waitpid(self.pid, 0)

        rmtree(self.tmpd)


# zsrv is test fixture to run a test wrt particular ZODB storage server.
#
# It currently yields FileStorage and ZEO.
#
# Clients should use zsrv.zurl to connect to the storage.
# See TestZSrv and its children classes for details.
@pytest.fixture(params=[TestFileStorage, TestZEOSrv],
                ids = lambda _: _.idname)
def zsrv(request): # -> ~TestZSrv
    nfail = request.session.testsfailed
    zsrv = request.param()

    yield zsrv

    # see if current test failed
    # https://stackoverflow.com/a/43268134/9456786
    failed = False
    if request.session.testsfailed > nfail:
        failed = True

    # dump server logs on test failure
    if failed:
        print()
        print(zsrv.all_logs())

    zsrv.teardown()
