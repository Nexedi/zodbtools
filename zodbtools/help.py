# -*- coding: utf-8 -*-
# zodbtools - help topics
# Copyright (C) 2017-2018  Nexedi SA and Contributors.
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

from collections import OrderedDict

# topic_name -> (topic_summary, topic_help)
topic_dict = OrderedDict()

help_zurl = """\
Almost every zodb command works with a database.
A database can be specified by way of providing URL for its storage.

The most general way to specify a storage is via preparing file with
ZConfig-based storage definition, e.g.

    %import neo.client
    <NEOStorage>
        master_nodes    ...
        name            ...
    </NEOStorage>

and using path to that file with zconfig:// schema:

    zconfig://<path-to-zconfig-storage-definition>

There are also following simpler ways:

- neo://<db>@<master>   for a NEO database
- zeo://<host>:<port>   for a ZEO database
- /path/to/file         for a FileStorage database

Please see zodburi documentation for full details:

http://docs.pylonsproject.org/projects/zodburi/
"""

help_tidrange = """\
Many zodb commands can be invoked on specific range of database history and
accept <tidrange> parameter for that. The syntax for <tidrange> is

    tidmin..tidmax

where tidmin and tidmax specify [tidmin, tidmax] range of transactions, ends
inclusive. Both tidmin and tidmax are optional and default to

    tidmin: 0   (start of database history)
    tidmax: +∞  (end of database history)

If a tid (tidmin or tidmax) is given, it has to be specified as follows:

    - a 16-digit hex number specifying transaction ID, e.g. 0285cbac258bf266
    - absolute timestamp, in RFC3339 or RFC822 formats
    - relative timestamp, e.g. yesterday, 1 week ago

Example tid ranges:

    ..                                  whole database history
    000000000000aaaa..                  transactions starting from 000000000000aaaa till latest
    ..000000000000bbbb                  transactions starting from database beginning till 000000000000bbbb
    000000000000aaaa..000000000000bbbb  transactions starting from 000000000000aaaa till 000000000000bbbb
    1985-04-12T23:20:50.52Z..2018-01-01T10:30:00Z
                                        transactions starting from 1985-04-12 at 23 hours
                                        20 minutes 50 seconds and 520000000 nano seconds
                                        in UTC till 2018-01-01 at 10 hours 30 minutes in UTC
    1_week_ago..yesterday               transactions from one week ago until yesterday.

In commands <tidrange> is optional - if it is not given at all, it defaults to
0..+∞, i.e. to whole database history.
"""

topic_dict['zurl']      = "specifying database URL",    help_zurl
topic_dict['tidrange']  = "specifying history range",   help_tidrange
