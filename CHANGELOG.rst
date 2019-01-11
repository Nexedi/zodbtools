Zodbtools change history
========================

0.0.0.dev7 (2019-01-11)
-----------------------

- Fix zodbtools to work with all ZODB3, ZODB4 and ZODB5 (`commit 1`__, 2__,
  3__, 4__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/425e6656
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/0e5d2f81
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7a94e312
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/8ff7020c

- Fix `zodb analyze` for the case when history range is empty (`commit 1`__,
  2__, 3__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/b4824ad5
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/d37746c6
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/474a0559

- Zodbtools is not yet Python3-ready (commit__), but we started to fix it
  step-by-step (`commit 1`__, 2__, 3__, 4__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7c5bb0b5
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7d24147b
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/55853615
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/79aa0c45
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/5e2ed5e7


0.0.0.dev6 (2018-12-30)
-----------------------

- `zodb analyze` can now work with any ZODB storage and supports analyzing a
  particular range of history (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/3ce22f28
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7ad9e1df

- Add help for specifying TID ranges (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/f7eff5fe

- Always close opened storages (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/9dbe70f3

0.0.0.dev5 (2018-12-13)
-----------------------

- Start to stabilize `zodb dump` format. The format is close to be stable now
  and will likely be changed, if at all, only in minor ways (`commit 1`__, 2__,
  3__, 4__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/75c03368
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/33230940
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7f0bbf7e
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/624aeb09

- Add `DumpReader` - class to read/parse input in `zodbdump` format (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/dd959b28

- Add `zodb commit` subcommand to commit new transaction into ZODB (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/960c5e17


0.0.0.dev4 (2017-04-05)
-----------------------

- Clarify licensing (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/9e4305b8
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/79cf177a

- Add `zodb` tool to drive all subcommands (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/984cfe22

- Add `zodb info` subcommand to print general information about a ZODB database
  (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/37b9fbde

- Switch to open ZODB storages by URL, not only via ZConfig files. URL support
  comes from `zodburi` (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/82b06413
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/bfeb1690


0.0.0.dev3 (2016-11-17)
-----------------------

- Move Nexedi version of `zodbanalyze` from ERP5 into zodbtools.

  Compared to original `zodbanalyze` Nexedi version is faster, prints not only
  total, but also current sizes, and supports running on bigger databases where
  keeping all working set to analyze in RAM is not feasible. It also supports
  analyzing a Repozo deltafs file directly.
  (`commit 1`__, 2__, 3__, 4__, 5__, 6__, 7__, 8__, 9__)

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/ab17cf2d
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/1e506a81
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/d86d04dc
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/5fd2c0eb
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/a9346784
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/1a489502
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/8dc37247
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/e4d4762a
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/2e834aaf


0.0.0.dev2 (2016-11-17)
-----------------------

- Add initial draft of `zodbdump` - tool to dump content of a ZODB database
  (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/c0a6299f
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/d955f79a

0.0.0.dev1 (2016-11-16)
-----------------------

- Initial release of zodbtools with `zodbcmp` (`commit 1`__, 2__, 3__).

  We originally tried to put `zodbcmp` into ZODB itself, but Jim Fulton asked__
  not to load ZODB with scripts anymore. This way zodbtools was created.

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/fd6ad1b9
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/66a03ae5
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/66946b8d
  __ https://github.com/zopefoundation/ZODB/pull/128#issuecomment-260970932
