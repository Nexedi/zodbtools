#!/usr/bin/env python
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
"""Zodb is a driver program for invoking zodbtools subcommands"""

from __future__ import print_function

from zodbtools import help as help_module

import getopt
import importlib
import sys


# command_name -> command_module
command_dict = {}

def register_command(cmdname):
    command_module = importlib.import_module('zodbtools.zodb' + cmdname)
    command_dict[cmdname] = command_module

for _ in ('analyze', 'cmp', 'commit', 'dump', 'info'):
    register_command(_)



def usage(out):
    print("""\
Zodb is a tool for managing ZODB databases.

Usage:

    zodb command [arguments]

The commands are:
""", file=out)

    cmdv = command_dict.keys()
    cmdv.sort()
    for cmd in cmdv:
        cmd_module = command_dict[cmd]
        print("    %-11s %s" % (cmd, cmd_module.summary), file=out)

    print("""\

Use "zodb help [command]" for more information about a command.

Additional help topics:
""", file=out)

    # NOTE no sorting here - topic_dict is pre-ordered
    for topic, (topic_summary, _) in help_module.topic_dict.items():
        print("    %-11s %s" % (topic, topic_summary), file=out)

    print("""\

Use "zodb help [topic]" for more information about that topic.
""", file=out)


# help shows general help or help for a command/topic
def help(argv):
    if len(argv) < 2:   # help topic ...
        usage(sys.stderr)
        sys.exit(2)

    topic = argv[1]

    # topic can either be a command name or a help topic
    if topic in command_dict:
        command = command_dict[topic]
        command.usage(sys.stdout)
        sys.exit(0)

    if topic in help_module.topic_dict:
        _, topic_help = help_module.topic_dict[topic]
        print(topic_help)
        sys.exit(0)

    print("Unknown help topic `%s`.  Run 'zodb help'." % topic, file=sys.stderr)
    sys.exit(2)


def main():
    try:
        optv, argv = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    if len(argv) < 1:
        usage(sys.stderr)
        sys.exit(2)

    command = argv[0]

    # help on a topic
    if command=="help":
        return help(argv)

    # run subcommand
    command_module = command_dict.get(command)
    if command_module is None:
        print('zodb: unknown subcommand "%s"' % command, file=sys.stderr)
        print("Run 'zodb help' for usage.", file=sys.stderr)
        sys.exit(2)

    return command_module.main(argv)


if __name__ == '__main__':
    main()
