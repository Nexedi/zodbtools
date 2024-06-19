# -*- coding: utf-8 -*-
# Copyright (C) 2024  Nexedi SA and Contributors.
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

import multiprocessing
import os
import shutil

from golang import func, defer
from persistent import Persistent
from persistent.mapping import PersistentMapping
import pytest
import transaction
from ZODB import DB
from ZODB.FileStorage import FileStorage
from ZODB.utils import u64, p64
from zodbtools.zodbtraverse import Explorer, Reporter, SQLiteManager


class Populator(object):
  """Populate ZODB with persistent objects.

  Different population strategies aim to test the integrity
  of the graph traversal algorithm.
  """

  def __init__(self):
    self.object_list = []
  
  def __call__(self, root):
    self.object_list.append(root)
    self._populate(root)
    transaction.commit()
    return set(map(lambda o: o._p_oid, self.object_list))

  def _addObject(self, name, referee, obj_factory=Persistent):
    obj = obj_factory()
    referee[name] = obj  # assert type(referee) is PersistentMapping
    self.object_list.append(obj)
    return obj

  def _populate(self, root):
    """Assign persistent objects to DB root"""


class PSimple(Populator):
  """Ensure traversal finds all objects in a flat simple DB"""
  def _populate(self, root):
    for i in range(100):
      self._addObject("object{}".format(i), root)


class PNested(Populator):
  """Ensure traversal finds all objects in a nested DB"""
  def _populate(self, root):
    def populate(referee, depth=0):
      if depth > max_depth:
        return
      for i in range(5):
        obj = self._addObject("object{}".format(i), referee, PersistentMapping)
        populate(obj, depth + 1)

    max_depth = 3
    populate(root)


class PDuplication(Populator):
  """Ensure traversal doesn't add duplicates to SQLite DB.

  In case it would try to add duplicates (in case the duplicate-finding
  algorithm would be bad), the python SQLite3 adapter would raise an
  error, because OID has the 'UNIQUE' constraint."""

  def _populate(self, root):
    o0 = self._addObject("o0", root, PersistentMapping)
    o1 = self._addObject("o1", o0, PersistentMapping)
    o1.o0 = o0  # cyclic reference to add OID duplicates
    o2 = self._addObject("o2", o1, PersistentMapping)
    o2.o0 = o0  # more cyclic references
    o2.o1 = o1
    o0.o2 = o2


class POrphan(Populator):
  """Ensure traversal only finds not-orphaned still refered-to objects."""
  def _populate(self, root):
    obj = self._addObject("t", root, PersistentMapping)
    obj.orphan = Persistent()
    transaction.commit()  # ensure objects is registered in DB
    assert obj._p_oid 
    obj.orphan = None  # de-refer it, so that traversal won't find it


@pytest.mark.parametrize("populator", (Populator, PSimple, PNested, PDuplication, POrphan))
def test_traverse(populator, setup_testdb, tmp_path):
  """Ensure graph traversal finds all reachable objects in DB.

  populator: How to populate the DB.

  This is a basic end-to-end test, that doesn't unit test the inner
  functionalities of the traversal algorithm.
  """
  zurl, oid_set, tid = setup_testdb(populator)
  e = Explorer(zurl, tmp_path, tid); e()
  assert oid_set == getOIDSet(e)


@pytest.mark.parametrize("populator,max_iteration_count", (((PSimple, 5), (PNested, 200), (PDuplication, 4), (POrphan, 1))))
def test_checkpoint(populator, max_iteration_count, setup_testdb, tmp_path, monkeypatch):
  """Ensure when stopping the process & restarting it from a checkpoint, the result is still correct.

  populator: How to populate the DB.
  max_iteration_count: How many iterations are allowed until a restart from a checkpoint
    is forced. For a larger DB, we need to use higher numbers so that tests aren't too slow.
    For a smaller DB, we need small enough numbers, so that the traversal is really restarted
    from a checkpoint before it finishes to ensure we really test that restarting from a
    checkpoint works correctly.
  """

  zurl, oid_set, tid = setup_testdb(populator)
  _prepare_explorer(max_iteration_count, monkeypatch)

  def traverse(is_first):
    """Restart traversal until it is finished"""
    e = Explorer(zurl, tmp_path, tid)
    checkpoint_path = "{}/state.pickle".format(tmp_path)
    kwargs = {} if is_first else {'checkpoint_path': checkpoint_path}
    # Put traversal into dedicated process to not kill testing process itself
    p = multiprocessing.Process(target=e, kwargs=kwargs)
    p.start()
    p.join()
    if p.exitcode == 0: # traversal is finished
      # Restart again to set 'e.table_name' => data is losed because we
      # were running the function in a subprocess.
      e(checkpoint_path=checkpoint_path)
      return e, is_first
    else:  # == killed -> we need to restart our process
      return traverse(False)

  e, is_first = traverse(True)
  assert not is_first, "Traversal didn't restart from a checkpoint"  # flag useless tests
  assert oid_set == getOIDSet(e)


def _prepare_explorer(max_iteration_count, monkeypatch):
  """Monkey patch 'Explorer' for reliable tests of the checkpoint restoring feature."""
  sete = lambda k, v: monkeypatch.setattr(Explorer, k, v)  # set explorer

  # patch explorer that it auto-kills itself after 'max_iteration_count'
  # iterations. In this way we can simulate a restarting of the traversal
  # process.
  Explorer_iteration1 = Explorer._iteration1
  def iteration1(self, *args, **kwargs):
    try:
      self._itercount += 1
    except AttributeError:
      self._itercount = 1
    if self._itercount > max_iteration_count:
      # Don't raise an exception and don't use sys.exit to _not_
      # trigger graceful cleanup (= except) code - we want to test
      # the real case where a process is killed by the OS (for instance
      # due to a full RAM) and neither has the chance for a clean shutdown.
      # We want to be sure, that even in these cases the final result is
      # still correct.
      os._exit(os.EX_OSERR)
    return Explorer_iteration1(self, *args, **kwargs)
  sete("_iteration1", iteration1)

  # Reduce batch processing size, so that each iteration only proceeds
  # few objects and we need to restart the process multiple times from
  # a checkpoint. If we wouldn't do this, in most cases the database
  # could be dumped before the explorer kills itself and we couldn't test
  # the checkpoint feature.
  sete("MIN_Q_SIZE", 1)
  sete("OID_BATCH_PROCESS_SIZE", 1)
  # must be smaller than 'max_iteration_count' to not loop forever
  sete("CHECKPOINT_FREQUENCY", max((1, int(max_iteration_count / 2))))


def test_report(setup_testdb, tmp_path):
  """Test reporting difference between two tables works"""
  zurl0, oid_set, tid0 = setup_testdb(PSimple)
  e0 = Explorer(zurl0, tmp_path, tid0); e0()
  assert oid_set == getOIDSet(e0)
  zurl1, tid1 = _forkdb(zurl0)
  e1 = Explorer(zurl1, tmp_path, tid1); e1()
  assert oid_set != getOIDSet(e1), "fork is the same as upstream"
  r = Reporter(zurl1, tmp_path, tid1)
  report, report_path = r()
  assert os.path.exists(report_path), "report file not created"
  assert len(report.split()) > 100, "too few differences detected!"
  for i in range(1, 101):
    assert "oid: {}".format(i) in report, "missing differing OID: %s" % i


@func
def _forkdb(upstream_zurl):
  """Copy filestorage to new storage and de-refer all objects to create a diff to upstream"""
  path = newzurl = "{}/fork.fs".format("/".join(upstream_zurl.split('/')[:-1]))
  shutil.copy(upstream_zurl, newzurl)
  stor = FileStorage(path); defer(stor.close)
  db = DB(stor); defer(db.close)
  conn = db.open(); defer(conn.close)
  root = conn.root()
  for k, v in root.items():
    root[k] = None
  transaction.commit()
  tid = u64(stor.lastTransaction()) + 1
  return newzurl, tid


@pytest.fixture
def setup_testdb(tmp_path):
  """Provides function to setup ZODB storage to be used for traversal test."""
  @func
  def _(populator=PSimple):
    zurl = path = "{}/data.fs".format(tmp_path)
    stor = FileStorage(path); defer(stor.close)
    db = DB(stor); defer(db.close)
    conn = db.open(); defer(conn.close)
    root = conn.root()
    oid_set = populator()(root)
    tid = u64(stor.lastTransaction()) + 1
    return zurl, oid_set, tid
  return _


@func
def getOIDSet(e):
  """Fetch all OIDs that has been committed to SQLite DB during ZODB traversal"""
  db_manager = SQLiteManager(e.workspace); defer(db_manager.close)
  return set(map(p64, db_manager.getOIDIterator(e.table_name)))
