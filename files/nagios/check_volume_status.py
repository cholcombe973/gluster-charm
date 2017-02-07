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

import argparse

from glusternagios import utils
from glusternagios import glustercli


def getVolumeStatus(args):
    exitstatus = 0
    message = ""
    try:
        volumes = glustercli.volumeInfo(args.volume)
        if volumes.get(args.volume) is None:
            exitstatus = utils.PluginStatusCode.CRITICAL
            message = "CRITICAL: Volume not found"
            return exitstatus, message
        elif volumes[args.volume]["volumeStatus"] == (glustercli.
                                                      VolumeStatus.ONLINE):
            exitstatus = utils.PluginStatusCode.OK
            message = "OK: Volume : %s type - Volume is up" % \
                      (volumes[args.volume]["volumeType"])
        elif volumes[args.volume]["volumeStatus"] == (glustercli.
                                                      VolumeStatus.OFFLINE):
            exitstatus = utils.PluginStatusCode.CRITICAL
            message = "CRITICAL: Volume : %s type is stopped" % \
                      (volumes[args.volume]["volumeType"])
    except glustercli.GlusterLockedException as e:
        out = ("UNKNOWN: Glusterd cannot be queried. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, out
    except glustercli.GlusterCmdFailedException as e:
        out = ("WARNING: Command execution failed. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.WARNING, out

    return exitstatus, message


def getVolumeQuotaStatus(args):
    try:
        qstatus = glustercli.volumeQuotaStatus(args.volume)
    except glustercli.GlusterLockedException as e:
        out = ("UNKNOWN: Glusterd cannot be queried. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, out
    except glustercli.GlusterCmdFailedException as e:
        out = ("QUOTA: Quota status could not be determined. %s"
               % '.'.join(e.err))
        return utils.PluginStatusCode.WARNING, out

    returnMsg = "QUOTA:"
    if qstatus.get("hard_ex_dirs"):
        hard_limit_ex = ', '.join(qstatus['hard_ex_dirs'])
        returnMsg += ("hard limit reached on %s; " % hard_limit_ex)
    if qstatus.get('soft_ex_dirs'):
        soft_limit_ex = ', '.join(qstatus['soft_ex_dirs'])
        returnMsg += ("soft limit exceeded on %s" % soft_limit_ex)
    if (returnMsg.endswith(';')):
        returnMsg = returnMsg[:-1]

    if qstatus['status'] == glustercli.VolumeQuotaStatus.SOFT_LIMIT_EXCEEDED:
        return utils.PluginStatusCode.WARNING, returnMsg
    elif (qstatus['status'] ==
          glustercli.VolumeQuotaStatus.HARD_LIMIT_EXCEEDED):
        return utils.PluginStatusCode.CRITICAL, returnMsg
    elif qstatus['status'] == glustercli.VolumeQuotaStatus.DISABLED:
        return utils.PluginStatusCode.OK, "QUOTA: not enabled or configured"
    else:
        return utils.PluginStatusCode.OK, "QUOTA: OK"


def getVolumeSelfHealSplitBrainStatus(args):
    try:
        volume = glustercli.volumeHealSplitBrainStatus(args.volume)
    except glustercli.GlusterLockedException as e:
        out = ("UNKNOWN: Glusterd cannot be queried. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, out
    except glustercli.GlusterCmdFailedException as e:
        out = ("Volume split brain status could not be determined. %s"
               % '.'.join(e.err))
        return utils.PluginStatusCode.WARNING, out

    if volume.get(args.volume) is None:
        exitstatus = utils.PluginStatusCode.UNKNOWN
        message = "UNKNOWN: Volume self heal split-brain info not found"
    else:
        if (volume[args.volume]['status'] == glustercli.
                VolumeSplitBrainStatus.NOTAPPLICABLE):
            exitstatus = utils.PluginStatusCode.OK
            message = "Volume is not of replicate type"
        elif (volume[args.volume]['status'] == glustercli.
                VolumeSplitBrainStatus.OK):
            exitstatus = utils.PluginStatusCode.OK
            message = "No split brain state entries found."
        elif (volume[args.volume]['status'] == glustercli.
                VolumeSplitBrainStatus.SPLITBRAIN):
            exitstatus = utils.PluginStatusCode.CRITICAL
            message = ("%s entries in split-brain state found."
                       % (volume[args.volume]['unsyncedentries']))
    return exitstatus, message


def getVolumeGeoRepStatus(args):
    try:
        volume = glustercli.volumeGeoRepStatus(args.volume)
    except glustercli.GlusterLockedException as e:
        out = ("UNKNOWN: Glusterd cannot be queried. %s" % '.'.join(e.err))
        return utils.PluginStatusCode.UNKNOWN, out
    except glustercli.GlusterCmdFailedException as e:
        out = ("Geo replication status could not be determined. %s"
               % '.'.join(e.err))
        return utils.PluginStatusCode.WARNING, out

    if volume.get(args.volume) is None:
        exitstatus = utils.PluginStatusCode.UNKNOWN
        message = "UNKNOWN: Volume info not found"
    else:
        exitstatus = utils.PluginStatusCode.OK
        message = "Session status:"
        detail = "Details:"
        for slavename, slave_dict in volume[args.volume]['slaves'].iteritems():
            message += ("%s - %s " % (slavename,
                                      slave_dict['status']))
            detail += ("%s - %s " % (slavename,
                                     slave_dict['detail']))
            if slave_dict['status'] == glustercli.GeoRepStatus.FAULTY:
                exitstatus = utils.PluginStatusCode.CRITICAL
            elif (slave_dict['status']
                  in [glustercli.GeoRepStatus.PARTIAL_FAULTY,
                      glustercli.GeoRepStatus.STOPPED,
                      glustercli.GeoRepStatus.NOT_STARTED]
                  and exitstatus == utils.PluginStatusCode.OK):
                exitstatus = utils.PluginStatusCode.WARNING
        if exitstatus != utils.PluginStatusCode.OK:
            message += "\n" + detail
        if len(volume[args.volume]['slaves']) == 0:
            message += "No active sessions found"
    return exitstatus, message


def parse_input():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--volume", action="store",
                        required=True,
                        help="Name of the volume for status")
    parser.add_argument("-t", "--type", action="store",
                        default="info",
                        dest="type",
                        help="Type of status to be shown. Possible values:",
                        choices=["info", "quota", "self-heal", "geo-rep"])
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_input()
    if args.type == "info":
        exitstatus, message = getVolumeStatus(args)
    if args.type == "quota":
        exitstatus, message = getVolumeQuotaStatus(args)
    if args.type == "self-heal":
        exitstatus, message = getVolumeSelfHealSplitBrainStatus(args)
    if args.type == "geo-rep":
        exitstatus, message = getVolumeGeoRepStatus(args)
    print message
    exit(exitstatus)
