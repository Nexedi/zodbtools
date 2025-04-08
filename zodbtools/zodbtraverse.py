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

from __future__ import print_function

import abc
import argparse
from datetime import datetime
from collections import deque
import operator
import logging
import pickle
import sqlite3
import six
import sys

from golang import func, defer
from ZODB.utils import u64, p64, get_pickle_metadata
from ZODB.serialize import referencesf
from ZODB import POSException, DB
from zodbtools.util import storageFromURL

__doc__ = """zodbtraverse: traverse database graph from root to find all reach- & loadable OIDs.

Two dumps can be compared, so that this tool helps to find out if two databases (or the
same database at a different time) have the same set of OIDs or which OIDs exactly differ.

If one wants to find out if the same OIDs are still reachable after same changes
to the ZODB storage (e.g. garbage collection, ...), one can run this tool before
applying these changes and run the tool again after applying these changes and then
finally compare these two states. To have reliable results, one should use the same
TID when running the tool before and after the changes to the database as otherwise
it's not clear if OIDs are simply no longer reachable, because objects were dereferred
or if there is indeed a problem of no-longer reach- or loadable objects that used
to be reach- or loadable.

NOTE The comparison between two different ZODB databases is currently somewhat limited,
as information regarding differing objects is only fetched wrt one of the two ZODB
databases within the same report call.
"""

summary = "traverse ZODB database & dump all reach- & loadable OIDs to SQLite DB"

def usage(stream, msg=None):
  if msg:
    print(msg, file=stream)
    print(file=stream)
  print(__doc__ % {"program": "zodb traverse"}, file=stream)


def main(argv):
  p = argparse.ArgumentParser(
    description=(
      "With this program you can compare the reachable OIDs of a ZODB "
      "storage at different states. "
      "Dump all reachable OIDs of a ZODB storage via the "
      "'--dump' flag & compare 2 dumps via the '--report' flag."
    )
)
  _ = p.add_argument
  # General arguments
  _("-z", "--zurl", help="URI of ZODB storage", required=True)
  _("-w", "--workspace", help="path to a directory where program files persist", required=True)
  _("-t", "--tid", help="TID of ZODB state that should be dumped")
  # Dump arguments
  _("-d", "--dump", action="store_true", help="dump reachable ZODB OIDs to SQLite DB")
  _("-c", "--checkpoint_path", help="path to pickle from which to resume ZODB dump")
  # Report arguments
  _("-r", "--report", action="store_true", help="compare 2 ZODB OID dumps")
  _("-t0", "--table0", help="name of first OID dump table to be compared")
  _("-t1", "--table1", help="name of second OID dump table to be compared")
  args = p.parse_args(argv[1:])

  if args.dump:
    logger.info("dump current ZODB state...")
    Explorer(args.zurl, args.workspace, args.tid)(args.checkpoint_path)

  if args.report:
    logger.info("report diff between two tables...")
    Reporter(args.zurl, args.workspace, args.tid)(args.table0, args.table1)


logger = logging.getLogger("dbstate")
logger.addHandler(logging.StreamHandler())
logger.setLevel(level=logging.INFO)


@six.add_metaclass(abc.ABCMeta)
class WithDB(object):
  """Helper to access ZODB & SQLite metadata table"""

  def __init__(self, zurl, workspace, tid=None):
    self.zurl = zurl
    self.workspace = workspace
    self.tid = tid and p64(int(tid))
    logger.addHandler(logging.FileHandler("%s/py.log" % self.workspace))

  @func
  def __call__(self, *args, **kwargs):
    # Setup SQLite conn
    self.db_manager = SQLiteManager(self.workspace); defer(self.db_manager.close)

    # Setup ZODB conn
    logger.info("connect to storage with zurl %s" % self.zurl)
    self.storage = storageFromURL(self.zurl); defer(self.storage.close)
    self.db = DB(self.storage); defer(self.db.close)
    self.conn = self.db.open(); defer(self.conn.close)

    return self._main(*args, **kwargs)
  
  @abc.abstractmethod
  def _main(self, *args, **kwargs):
    raise NotImplementedError()

  def getTID(self):
    return self.tid or (self._setTID() or self.tid)

  def _setTID(self):
    assert not self.tid
    self.tid = self.storage.lastTransaction()
    logger.info("set tid to %s" % u64(self.tid))

  def _load(self, oid):
    try:
      return self.storage.loadBefore(oid, self.getTID())
    except POSException.POSKeyError:
      return None


class Explorer(WithDB):
  """Traverse graph of ZODB & dump all found OIDs into a SQLite table."""

  # The following constants all define numbers that tune the algorithm
  # to find a balance between RAM consumption & performance. Unless
  # there are problems in this balance (or one wants to run specific
  # tests), these numbers don't need to be touched.

  # Minimum size of the 'q' deque, that contains OIDs that were
  # recently loaded & must further proceed to the 'qload' deque.
  # Don't fill up 'q' with too many OIDs, but add more OIDs than
  # from only 1 object, to speed up the algorithm.
  MIN_Q_SIZE = 1000
  # 'CHECKPOINT_FREQUENCY' sets after how many iterations a new
  # checkpoint should be dumped again. This doesn't happen at
  # each iteration to speed-up the program.
  CHECKPOINT_FREQUENCY = 50
  # How many OIDs are parsed at each iteration from the 'q' deque
  # to the 'qload' deque.
  OID_BATCH_PROCESS_SIZE = 500

  def _main(self, checkpoint_path=None):
    if checkpoint_path:
      q, qload, iteration_index = self.restoreCheckpoint(checkpoint_path)
    else:
      self.table_name = self.db_manager.addNewTable()
      q, qload = deque([self.conn.root()._p_oid]), deque()
      iteration_index = 0

    self.traverse(q, qload, iteration_index)

    self.db_manager.proceedDeferredInserts()  # to have the correct number of OIDs logged
    logger.info("finished traversal (collected {} OIDs)".format(
      self.db_manager.getObjectCount(self.table_name)))

  def traverse(self, q, qload, iteration_index):
    """Traverse ZODB tree starting from top application.

    This methods dumps all reachable & loadable OIDs into a
    SQLite table. An OID that's reachable but unloadable is
    skipped. An OID that's loadable but not reachable is
    skipped.
    """
    while q or qload:
      self._iteration1(q, qload, iteration_index)
      iteration_index += 1

  def _iteration1(self, q, qload, iteration_index):
    """Proceed 1 iteration in ZODB traversal"""
    loadToQ = 0
    temp_oid_list = []

    # improve performance: Batch process OIDs to reduce calls to
    # SQLite DB
    for _ in range(self.OID_BATCH_PROCESS_SIZE):
      try:
        oid = q.pop()
      # Save RAM: Keep queues as small as possible by only
      # adding new OIDs when we don't have any OIDs to proceed
      # anymore.
      except IndexError:
        loadToQ = 1
        break
      if oid not in temp_oid_list and oid not in qload:
        temp_oid_list.append(oid)

    unique_oid_list = self.db_manager.getNotYetExistObjectList(
      list(map(u64, temp_oid_list)), self.table_name)
    qload.extend(map(p64, unique_oid_list))

    if loadToQ:
      self.loadToQ(q, qload, iteration_index)

    self.saveCheckpoint(q, qload, iteration_index)

  def loadToQ(self, q, qload, iteration_index):
    """Batch load OIDs from qload and put references into q"""
    n = 0
    while n < self.MIN_Q_SIZE:
      try:
        nextoid = qload.pop()
      except IndexError:
        break
      for oid in self.load(nextoid, iteration_index):
        if oid not in q:
          q.append(oid)
          n += 1

  def load(self, oid, iteration_index):
    """Load OID from storage, put OID in SQLiteDB & return list of references"""
    data = self._load(oid)
    if data is None:
      return []  # don't put oid into sql table if unloadable
    self.db_manager.addObject(u64(oid), iteration_index, self.table_name)
    return referencesf(data[0])

  def restoreCheckpoint(self, checkpoint_path):
    """Restore previous runtime state from pickle"""
    with open(checkpoint_path, 'r') as f:
      tid, self.table_name, iteration_index, q, qload = pickle.load(f)

    # Just check for consistency
    tid = p64(tid)
    if self.tid:
      assert tid == self.tid, "Inconsistent requested TID: checkpoint TID & user defined TID differ"
    self.tid = tid

    # In case we resume from a checkpoint, we need to drop any data
    # from the SQLite DB that has been added after the checkpoint.
    # Otherwise the correctness of the traversal can't be guaranteed:
    # it could be that an OID is already added to SQLite, but we don't
    # have its referred OIDs in the restored 'q' yet, because the script
    # exited before the checkpoint could be created. In this case a part
    # of the graph would be lost, because we wouldn't traverse this part
    # anymore as we already have the OID in the SQLite DB and the duplication
    # finder would filter it.
    self.db_manager.dropObjectsNewerThan(iteration_index, self.table_name)

    logger.info("Restored checkpoint %s" % self._formatCheckpoint(iteration_index, q, qload))

    # The pickled iteration_index is the index of the last fully run
    # traverse iteration - this means the next iteraton must be the next index.
    return q, qload, iteration_index + 1

  def saveCheckpoint(self, q, qload, iteration_index):
    """Save current runtime state via pickle to hard drive"""
    # We don't want to save checkpoints after each iteration
    # to not make the script slow due to too many IO operations.
    if iteration_index % self.CHECKPOINT_FREQUENCY != 0:
      return
    # Ensure all SQL data inside the RAM is proceeded to not loose any data.
    self.db_manager.proceedDeferredInserts()
    # 'referencesf' returns 'list[str, zodbpickle.binary]'. During traversal it
    # doesn't matter if an OID is encoded as a str or a binary, but we can't pickle
    # 'binary', so when creating checkpoints we need to convert everything to 'str'.
    oid2str = lambda oid: p64(u64(oid))
    q, qload = (deque(map(oid2str, que)) for que in (q, qload))
    # Finally dump checkpoint to storage
    with open("%s/state.pickle" % self.workspace, "w") as f:
      pickle.dump((u64(self.getTID()), self.table_name, iteration_index, q, qload), f)
    logger.info("Saved checkpoint %s" % self._formatCheckpoint(iteration_index, q, qload))

  def _formatCheckpoint(self, iteration_index, q, qload):
    return "@ iteration {} ({} OIDs in SQLite DB; len(q) = {}; len(qload) = {})".format(
        iteration_index, self.db_manager.getObjectCount(self.table_name), len(q), len(qload))


class Reporter(WithDB):
  """Compare two previously dumped OID tables & dump report about their delta into a text file
  
  As we only dump OIDs and no "real" data into the SQLite DB, the comparison merely
  checks if or if not the OIDs are the same, but doesn't compare the content of the
  objects themselves.

  NOTE The 'Reporter' currently assumes that both tables point to the same ZODB database
  and that any object that differs between both tables can be loaded with the same (and
  only one) ZODB database, that is specified via the 'zurl' parameter of the reporters
  initialization.
  """

  def _main(self, table0=None, table1=None):
    report_list = []
    if table0 is None or table1 is None:
      table0, table1 = self.db_manager.getLastTwoTables()

    for t0, t1 in ((table0, table1), (table1, table0)):
      report_list.extend(self.createTableDiffReport(t0, t1))
      report_list.append("\n" * 3)

    report = "\n".join(report_list)

    report_path = '%s/report_diff_%s_with_%s.txt' % (self.workspace, table0, table1)
    with open(report_path, 'w') as f:
      f.write(report)

    return report, report_path

  def createTableDiffReport(self, table0, table1):
    """Report all OIDs that are in table0, but not in table1"""
    report_list = ["Objects present in %s, but not in %s\n" % (table0, table1)]
    delta_iter0 = self.db_manager.getTableDeltaIterator(table0, table1)
    object_diff_dict = {}
    for delta_oid in delta_iter0:
      self.createObjectReport(delta_oid, object_diff_dict)
    for klass, object_report_list in object_diff_dict.items():
      report_list.extend(
        ["\n  objects with type '%s':\n" % klass] + object_report_list
      )
    return report_list

  def createObjectReport(self, delta_oid, object_diff_dict):
    """Create report for 1 OID"""
    rlist = ["    oid: %s" % delta_oid]

    # NOTE This assumes that both tables refer to the same database,
    # e.g. that an object that differs between both tables can be
    # loaded with the same database (instead of each table having its
    # own database).
    data = self._load(p64(delta_oid))
    if data is None:
      klass = "unknown (couldn't load object from storage)"
    else:
      klass = ".".join(get_pickle_metadata(data[0]))

    try:
      object_report_list = object_diff_dict[klass]
    except KeyError:
      object_report_list = object_diff_dict[klass] = []

    r = "\t".join(rlist)
    object_report_list.append(r)
    return "\t".join(rlist)


class SQLiteManager(object):
  """Provides API to SQLite DB where ZODB metadata is stored"""

  # How many rows should be batch inserted with one query.
  # We add more than 1 row in a query to speed up performance.
  MIN_INSERT_QUERY_SIZE = 500

  def __init__(self, workspace):
    self.db_path = "%s/meta.db" % workspace
    self.conn = sqlite3.connect(self.db_path)
    self._deferred_table_list = []

  def getOIDIterator(self, table_name):
    for res in self.queryIterator("SELECT oid FROM %s;" % table_name):
      yield res[0]

  def getObjectCount(self, table_name):
    return self.query("SELECT COUNT(*) FROM %s;" % table_name).fetchone()[0]

  def getNotYetExistObjectList(self, oid_list, table_name):
    """Return list of OIDs that aren't already in the SQLite DB yet"""
    get0 = operator.itemgetter(0)
    deferred_oid_list = list(map(get0, self._getDeferredRowList(table_name)))
    oid_list = list(filter(lambda oid: oid not in deferred_oid_list, oid_list))
    if not oid_list:
      return []
    condition = ",".join(list(map(str, oid_list)))
    query = "SELECT oid FROM {} WHERE oid IN ({});".format(table_name, condition)
    r = self.query(query).fetchall()
    duplicate_oid_tuple = tuple(map(get0, r))
    return list(filter(lambda oid: oid not in duplicate_oid_tuple, oid_list))

  def getTableDeltaIterator(self, table0, table1):
    def _():
      query = "SELECT oid FROM %s EXCEPT SELECT oid FROM %s;" % (table0, table1)
      for r in self.queryIterator(query):
        yield r[0]

    return _()

  def getLastTwoTables(self):
    i = self.getTableNameIterator()
    try:
      return tuple(str(next(i)[0]) for _ in range(2))
    except StopIteration:
      raise RuntimeError("There are less than 2 tables in the DB!")

  def getTableNameIterator(self):
    return self.queryIterator(
      "SELECT name FROM sqlite_master "
      "WHERE type IN ('table','view') "
      "AND name NOT LIKE 'sqlite_%' "
      "ORDER BY 1 DESC"
    )

  def dropObjectsNewerThan(self, iteration_index, table_name):
    self.query(
      "DELETE FROM %s WHERE iteration_index > %s;" % (table_name, iteration_index),
      commit=True)

  def addNewTable(self):
    datestr = datetime.now().isoformat()[:-5].replace(" ", "_").replace(":", "_").replace("-", "_").replace(".", "_")
    table_name = "objects_%s" % datestr
    cur = self.conn.cursor()
    logger.info("create table %s in db %s" % (table_name, self.db_path))
    cur.execute("CREATE TABLE %s (oid int UNIQUE, iteration_index int)" % table_name)
    self.conn.commit()
    return table_name

  def addObject(self, oid, iteration_index, table_name):
    self._maybeAddRows(table_name, (oid, iteration_index))

  def _maybeAddRows(self, table_name, row, min_size=None):
    # Defer adding rows, so that we add more than 1 row in
    # 1 SQL command to increase performance.
    row_list = self._getDeferredRowList(table_name)
    if row is not None:
      row_list.append(row)
    if len(row_list) >= (min_size or self.MIN_INSERT_QUERY_SIZE):
      self._addRows(table_name, row_list)
      setattr(self, self._tableNameToDeferredRowListName(table_name), [])

  def _addRows(self, table_name, row_list):
    if not row_list:
      return
    values = ",".join([("(%s)" % ",".join(list(map(str, r)))) for r in row_list])
    self.query("INSERT INTO {} VALUES {};".format(table_name, values), commit=True)

  def _getDeferredRowList(self, table_name):
    attr = self._tableNameToDeferredRowListName(table_name)
    try:
      return getattr(self, attr)
    except AttributeError:
      setattr(self, attr, [])
      self._deferred_table_list.append(table_name)
      return self._getDeferredRowList(table_name)

  def _tableNameToDeferredRowListName(self, table_name):
    return "_%s_row_list" % table_name

  def proceedDeferredInserts(self):
    for table_name in self._deferred_table_list:
      self._maybeAddRows(table_name, None, min_size=1)

  def close(self):
    self.proceedDeferredInserts()
    self.conn.close()

  def query(self, q, commit=False):
    cur = self.conn.cursor()
    try:
      r = cur.execute(q)
    except Exception:
      logger.warning("bad query: %s" % q)
      raise
    else:
      if commit:
        self.conn.commit()
      return r

  def queryIterator(self, *args, **kwargs):
    r = self.query(*args, **kwargs)

    def _():
      while 1:
        row = r.fetchone()
        if row is None:
          break
        yield row

    return _()