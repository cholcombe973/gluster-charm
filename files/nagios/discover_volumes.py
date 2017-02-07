#!/usr/bin/python
# discover_volumes.py -- nagios plugin for discovering
# logical gluster components
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
import json
import sys
import argparse

from glusternagios import utils
from glusternagios import glustercli


def discoverVolumes(volumeName, list):
    """
    This method helps to discover volumes list and volume info
    Parameters
    ----------
    list: Flag used for getting volume info. If the flag is 'True' then the
    method will return only list of volume names with volume Type and doesn't
    include the brick details. . If list is 'False' then returns the volume
    details with brick information.

    volumeName: Fetch information only for the given volume.
    Note: glustercli.volumeInfo(volName) command accept a volumeName. But if
    the volume name is not passed then it returns details about all the volumes
    in the cluster.
    Returns
    ---------
     Returns volume details in the following dictionary format
    {
     'vol-name' : {vol-details}
     'vol-name' : {vol-details}
     'vol-name' : {vol-details}
     ...
    }
    """
    resultlist = {}
    try:
        volumes = glustercli.volumeInfo(volumeName)
    except glustercli.GlusterLockedException as e:
        resultString = ("UNKNOWN: temporary error. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, resultString
    except glustercli.GlusterCmdFailedException as e:
        resultString = ("UNKNOWN: Failed to get the volume Information. "
                        "%s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, resultString
    for key, volume in volumes.iteritems():
        volDict = {}
        volDict['name'] = key
        volDict['type'] = volume['volumeType']
        if not list:
            volOptions = volume.get('options')
            if volOptions:
                quotaStatus = volOptions.get('features.quota')
                if quotaStatus == "on":
                    volDict['quota'] = quotaStatus
                geoRepStatus = volOptions.get('geo-replication.indexing')
                if geoRepStatus == "on":
                    volDict['geo-rep'] = geoRepStatus

            volDict['replicaCount'] = volume['replicaCount']
            volDict['bricks'] = []
            volDict['disperseCount'] = volume['disperseCount']
            volDict['redundancyCount'] = volume['redundancyCount']
            for brick in volume['bricksInfo']:
                brickproplist = brick['name'].split(':')
                volDict['bricks'].append({'brickaddress': brickproplist[0],
                                          'brickpath': brickproplist[1],
                                          'hostUuid': brick['hostUuid']})
        resultlist[key] = volDict
    resultString = json.dumps(resultlist)
    return utils.PluginStatusCode.OK, resultString


def get_arg_parser():
    parser = argparse.ArgumentParser(description="Discovery tool for "
                                                 "Gluster volumes")
    parser.add_argument('-l', '--list', action='store_true', dest='list',
                        help="Fetch only list of  volumes names")
    parser.add_argument('-v', '--volume', action='store', dest='volume',
                        type=str, help='Volume name')
    return parser


if __name__ == '__main__':
    args = get_arg_parser().parse_args()
    status, resultString = discoverVolumes(args.volume, args.list)
    print resultString
    sys.exit(status)
