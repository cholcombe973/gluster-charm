#!/usr/bin/python
# Copyright (C) 2014 Red Hat Inc
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA
#
from glusternagios import utils
from glusternagios import glustercli


def getClusterQuorumStatus():
    exitstatus = 0
    message = ""
    try:
        volumes = glustercli.volumeInfo()
    except glustercli.GlusterLockedException as e:
        out = ("UNKNOWN: temporary error. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, out
    except glustercli.GlusterCmdFailedException as e:
        out = ("Quorum status could not be determined. %s"
               % '.'.join(e.err))
        return utils.PluginStatusCode.WARNING, out

    quorumVolumes = []
    for volumename, volume in volumes.iteritems():
        if (volume.get('options') and
           volume.get('options').get('cluster.server-quorum-type')
           == "server"):
            quorumVolumes.append(volumename)
    if not quorumVolumes:
        exitstatus = utils.PluginStatusCode.UNKNOWN
        message = "Server quorum not turned on for any volume"
    else:
        exitstatus = utils.PluginStatusCode.OK
        message = ("Server quorum turned on for %s"
                   % (','.join(quorumVolumes)))
    return exitstatus, message


if __name__ == '__main__':
    exitstatus, message = getClusterQuorumStatus()
    print message
    exit(exitstatus)
