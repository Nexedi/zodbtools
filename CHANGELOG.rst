Zodbtools change history
========================

0.0.0.dev9 (2024-02-16)
-----------------------

- Add preliminary support for Python3 (`commit 1`__, 2__, 3__, 4__, 5__, 6__,
  7__, 8__, 9__, 10__, 11__, 12__, 13__, 14__, 15__, 16__, 17__, 18__, 19__,
  20__, 21__, 22__). Full py3 support depends on the completion of `bstr work`__
  in Pygolang.

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/2d94ae9d
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/00a534ef
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/62b21d01
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7a7370e6
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/a7eee284
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/1418c86f
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/b508f108
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/c5f20201
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/bc608aea
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/ddd5fd03
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/d3152c78
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/2f9e0623
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7851a964
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/2236aaaf
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/adec18bd
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/3cb93096
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/e825f80f
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/69dc6de1
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/b21fbe23
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/9861c136
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/65ebbe7b
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/7ae5ff8
  __ https://lab.nexedi.com/nexedi/pygolang/-/merge_requests/21

- Add new `zodb restore` command to restore database from `zodb dump` output
  (`commit 1`__, 2__, 3__, 4__, 5__, 6__, 7__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/67b42fa7
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/e7b82a96
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/b944e0e
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/4275f2e9
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/37786d10
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/1b480c93
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/a9853038

- `zodb dump` now supports `--pretty` option with a format to show pickles
  disassembly (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/80559a9

- Drop support for ZODB 3 (commit__). Only ZODB 4 and ZODB 5 remain to be supported.

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/c59a54ca

- `zodb info`: Provide "head" as subcommand to query last transaction of the database;
  Turn "last_tid" into deprecated alias for "head" (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/a2e4dd2

- Robustify `zodb commit` when handling object copies and reporting errors (`commit 1`__, 2__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/fa00c283
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/129afa67

- Add support to run tests under Nexedi testing infrastructure (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/518537ea


0.0.0.dev8 (2019-03-07)
-----------------------

- Support using absolute and relative time in tidrange.  One example usage is:
  ``zodb analyze data.fs 2018-01-01T10:30:00Z..yesterday`` (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/4037002c

- Python3 support progressed (`commit 1`__, 2__, 3__), but zodbtools does not
  support python3 yet. The test suite was extended to run on python3 (commit__)
  and also was extended to also run on ZODB with raw extensions from ongoing
  pull request `#183`__  (commit__).

  __ https://lab.nexedi.com/nexedi/zodbtools/commit/d6bde57c
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/f16ccfd4
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/b338d004
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/eaa3aec7
  __ https://github.com/zopefoundation/ZODB/pull/183
  __ https://lab.nexedi.com/nexedi/zodbtools/commit/c50bfb00


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
