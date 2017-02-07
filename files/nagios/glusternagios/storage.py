# Copyright 2014 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import os

import utils
from utils import CommandPath
import glustercli

lvsCmdPath = CommandPath("lvs",
                         "/sbin/lvs",
                         )
vgsCmdPath = CommandPath("vgs",
                         "/sbin/vgs",
                         )
pvsCmdPath = CommandPath("pvs",
                         "/sbin/pvs")
dfCmdPath = CommandPath("df", "/bin/df")


# Class for exception definition
class ExecCmdFailedException(Exception):
    message = "command execution failed"

    def __init__(self, rc=0, out=(), err=()):
        self.rc = rc
        self.out = out
        self.err = err

    def __str__(self):
        o = '\n'.join(self.out)
        e = '\n'.join(self.err)
        if o and e:
            m = o + '\n' + e
        else:
            m = o or e

        s = self.message
        if m:
            s += '\nerror: ' + m
        if self.rc:
            s += '\nreturn code: %s' % self.rc
        return s


# Creates a dict out of the given string
def _reduceToDict(out):
    for line in out:
        yield dict(pair.split('=') for pair in line.strip().split('^'))


# Parses the lvs command output and returns the required dictionary
def _parseLvs(output):
    return dict((x['LVM2_LV_PATH'], x) for x in _reduceToDict(output))


def _getLvsCmd():
    return [lvsCmdPath.cmd] + (" --unquoted --noheading " +
                               "--nameprefixes --separator ^ " +
                               "--nosuffix --units m -o " +
                               "lv_all,vg_name").split()


def _getVgsCmd():
    return [vgsCmdPath.cmd] + (" --unquoted --noheading " +
                               "--nameprefixes --separator ^ " +
                               "--nosuffix --units m -o " +
                               "vg_all,lv_path").split()


def _getPvsCmd():
    return [pvsCmdPath.cmd] + (" --unquoted --noheading " +
                               "--nameprefixes --separator ^ " +
                               "--nosuffix --units m -o " +
                               "pv_all,vg_name").split()


def _getLvDetailsCmd():
    return [pvsCmdPath.cmd] + (" --unquoted --noheading " +
                               "--nameprefixes --separator ^ " +
                               "--nosuffix --units m -o " +
                               "vg_name,pv_name,lv_name").split()


def _getdfDetailsCmd(brickName):
    return [dfCmdPath.cmd] + (" -ah " + brickName).split()


def getdf(brickName):
    rc, out, err = utils.execCmd(_getdfDetailsCmd(brickName))
    if rc:
        raise ExecCmdFailedException(err=[str(err)])

    return out


# Gets the list of logical volumes
def getLvs():
    rc, out, err = utils.execCmd(_getLvsCmd())
    if rc:
        raise ExecCmdFailedException(err=[str(err)])

    return _parseLvs(out)


# Parses the vgs coammnd output and returns the required dictionary
def _parseVgs(out):
    def _makeVgDict(x, y):
        y['LVM2_LV_PATH'] = [y['LVM2_LV_PATH']] if y['LVM2_LV_PATH'] else []
        if y['LVM2_VG_NAME'] in x:
            x[y['LVM2_VG_NAME']]['LVM2_LV_PATH'] += y['LVM2_LV_PATH']
        else:
            x[y['LVM2_VG_NAME']] = y
        return x

    return reduce(_makeVgDict, _reduceToDict(out), {})


# Gets the list of volume groups
def getVgs():
    rc, out, err = utils.execCmd(_getVgsCmd())
    if rc:
        raise ExecCmdFailedException(err=[str(err)])

    return _parseVgs(out)


# Parses the output of pvs command and returns the required dictionary
def _parsePvs(out):
    return dict((x['LVM2_PV_NAME'], x) for x in _reduceToDict(out))


# Gets the list of physical volumes
def getPvs():
    rc, out, err = utils.execCmd(_getPvsCmd())
    if rc:
        raise ExecCmdFailedException(err=[str(err)])

    return _parsePvs(out)


# Returns the mount point for the given path
def _getMountPoint(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)

    return path


# Gets the lv details
def _getLvDetails():
    rc, out, err = utils.execCmd(_getLvDetailsCmd())
    if rc:
        raise ExecCmdFailedException(err=[str(err)])

    return dict((x['LVM2_LV_NAME'], x) for x in _reduceToDict(out))


# Gets the brickwise mount points
def _getBrickMountPoints():
    mount_points = {}
    volumeInfo = glustercli.volumeInfo()
    for key in volumeInfo.keys():
        volume = volumeInfo[key]
        bricks = volume['bricks']
        for brick in bricks:
            mount_points[brick] = _getMountPoint(brick.split(":")[1])

    return mount_points


# Gets the list of all the proc mounts
def _getProcMounts():
    mounts = {}
    with open('/proc/mounts') as f:
        for line in f:
            arr = line.split()
            mounts[arr[0]] = arr[1]

    return mounts


# Gets the list of bricks for a given disk
def getBricksForDisk(diskName):
    # Get all the lv details
    lv_dict = _getLvDetails()

    # Reduce the LVs for the give device
    validLvs = {}
    for key in lv_dict.keys():
        if lv_dict[key]['LVM2_PV_NAME'] == diskName:
            validLvs[key] = lv_dict[key]

    # Get the brickwise mount points
    brick_mount_points = _getBrickMountPoints()

    # Get the list of all the mount points
    procmounts = _getProcMounts()

    # Get the mount points to find bricks for
    mount_points_to_check = []
    for key in validLvs.keys():
        searchname = "%s-%s" % (validLvs[key]['LVM2_VG_NAME'],
                                validLvs[key]['LVM2_LV_NAME'])
        for mount in procmounts.keys():
            if mount.endswith(searchname):
                mount_points_to_check.append(procmounts[mount])

    # Get the list of bricks
    bricks_list = []
    for key in brick_mount_points.keys():
        if brick_mount_points[key] in mount_points_to_check:
            bricks_list.append(key)

    return bricks_list


# Gets the brick's device name
def _getBrickDeviceName(brickName):
    brickDevices = {}
    volStatus = glustercli.volumeStatus("all", option="detail")
    bricks = volStatus['bricks']
    for brick in bricks:
        brick_dir = brick['brick']
        brickDevices[brick_dir] = brick['device']

    if brickName in brickDevices.keys():
        return brickDevices[brickName]
    else:
        return ""


# Gets the list of disks participating in the given brick
def getDisksForBrick(deviceName=None, brickName=None):
    if brickName is None and deviceName is None:
        return ""
    # Get the brick device name
    if deviceName is None:
        deviceName = _getBrickDeviceName(brickName)

    # Get the lv details
    lv_dict = _getLvDetails()

    # Get the disk name for the brick
    for key in lv_dict.keys():
        tmp_str = "%s-%s" % (lv_dict[key]['LVM2_VG_NAME'],
                             lv_dict[key]['LVM2_LV_NAME'])
        if deviceName.endswith(tmp_str):
            return lv_dict[key]['LVM2_PV_NAME']

    return ""


# gets the brick's device name using df command
def getBrickDeviceName(brickName):
    brickName = brickName.rstrip()
    if brickName is "":
        return ""
    dfOut = getdf(brickName)
    # The output will be similar to
    # ['Filesystem      Size  Used Avail Use% Mounted on',
    #  '/dev/vda1       485M   34M  426M   8% /boot']
    # need to parse to get the device name
    if len(dfOut) > 1:
        dfOutList = dfOut[1].split()
        if len(dfOutList) > 0:
            return dfOutList[0]
    return ""
