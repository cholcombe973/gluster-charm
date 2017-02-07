#!/usr/bin/python
# Copyright (C) 2015 Red Hat Inc
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


import os
import sys
import argparse
import logging

from glusternagios import utils
from glusternagios.utils import PluginStatusCode, PluginStatus

ONE_GB_BYTES = 1073741824.0


def _getMountPoint(path):
    mount = os.path.realpath(path)
    while not os.path.ismount(mount):
        mount = os.path.dirname(mount)
    return mount


def _parseProcMounts(filter=True):
    mountPoints = {}
    with open('/proc/mounts', 'r') as f:
        for line in f:
            if line.startswith("/") or not filter:
                mount = {}
                tokens = line.split()
                mount['device'] = tokens[0]
                mount['fsType'] = tokens[2]
                mount['mountOptions'] = tokens[3]
                mountPoints[tokens[1]] = mount
    return mountPoints


def _getStats(mountPoint):
    data = os.statvfs(mountPoint)
    total = (data.f_blocks * data.f_bsize) / ONE_GB_BYTES
    free = (data.f_bfree * data.f_bsize) / ONE_GB_BYTES
    used_percent = 100 - (100.0 * free / total)
    total_inode = data.f_files
    free_inode = data.f_ffree
    used_percent_inode = 100 - (100.0 * free_inode / total_inode)
    used = total - free
    used_inode = total_inode - free_inode
    return {'total': total,
            'free': free,
            'used_percent': used_percent,
            'total_inode': total_inode,
            'free_inode': free_inode,
            'used_inode': used_inode,
            'used': used,
            'used_percent_inode': used_percent_inode}


def _getOutputText(detail):
    template = "{mount_point} - {{space - free: {free:0.3f} GiB, " \
               "used: {used_percent:0.3f}%}}, {{inode - free: {free_inode}" \
               " , used: {used_percent_inode:0.3f}%}}"

    if detail['thinpool_size']:
        template += ", {{thinpool-data - free: {thinpool_free:.3f} GiB," \
                    " used: {thinpool_used_percent:.3f}%}}, " \
                    "{{thinpool-metadata - free: {metadata_free:.3f} GiB," \
                    " used: {metadata_used_percent:.3f}%}}"
    return template.format(**detail)


def _getPerfdata(detail, warn, crit):
    template = "{mount_point}={used_percent:.3f}%;{warn};{crit};0;{total:.3f}"\
               " {mount_point}.inode={used_percent_inode:.3f}%;{warn};{crit}" \
               ";0;{total_inode}"
    if detail['thinpool_size']:
        template += " {mount_point}.thinpool={thinpool_used_percent:.3f}%;" \
                    "{warn};{crit};0;{thinpool_size:.3f} {mount_point}." \
                    "thinpool-metadata={metadata_used_percent:.3f}" \
                    "%;{warn};{crit};0;{metadata_size:.3f}"
    return template.format(warn=warn, crit=crit, **detail)


def _getStatusInfo(detail, warn, crit):
    rc = PluginStatus.OK
    msg = []

    parameter = {'metadata_used_percent': ['thinpool-metadata',
                                           'metadata_used',
                                           'metadata_size', ' GiB'],
                 'thinpool_used_percent': ['thinpool-data', 'thinpool_used',
                                           'thinpool_size', ' GiB'],
                 'used_percent': ['space', 'used',
                                  'total', ' GiB'],
                 'used_percent_inode': ['inode', 'used_inode',
                                        'total_inode', '']}

    for k, v in parameter.iteritems():
        if not detail[k]:
            continue
        if k == 'used_percent_inode':
            m = "%s used %d / %d%s" % (v[0], detail[v[1]],
                                       detail[v[2]], v[3])
        else:
            m = "%s used %0.3f / %0.3f%s" % (v[0], detail[v[1]],
                                             detail[v[2]], v[3])
        if detail[k] >= crit:
            rc = PluginStatus.CRITICAL
            msg.append(m)
        elif detail[k] >= warn:
            if rc != PluginStatus.CRITICAL:
                rc = PluginStatus.WARNING
            msg.append(m)

    if rc == PluginStatus.OK:
        out = ''
    else:
        out = "mount point {mount_point} {{{msg}}}".format(msg=", ".join(msg),
                                                           **detail)
    return rc, out


def parse_input():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--warning", action="store",
                        required=True, type=int,
                        help="Warning threshold in percentage")
    parser.add_argument("-c", "--critical", action="store",
                        required=True, type=int,
                        help="Critical threshold in percentage")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-e", "--exclude", action="append", default=[],
                       help="exclude given interface")
    group.add_argument("-i", "--include", action="append", default=[],
                       help="add given interface for monitoring")
    args = parser.parse_args()
    return args


def getLvs():
    lvmCommand = ["lvm", "vgs", "--unquoted", "--noheading",
                  "--nameprefixes", "--separator", "$",
                  "--nosuffix", "--units", "m", "-o",
                  "lv_uuid,lv_name,data_percent,pool_lv,lv_attr,"
                  "lv_size,lv_path,lv_metadata_size,"
                  "metadata_percent,vg_name"]
    rc, out, err = utils.execCmd(lvmCommand)
    if rc != 0:
        logging.error(
            "lvm command failed.\nCommand=%s\nrc=%s\nout=%s\nerr=%s"
            % (lvmCommand, rc, out, err)
        )
        return None
    l = map(lambda x: dict(x),
            map(lambda x: [e.split('=') for e in x],
                map(lambda x: x.strip().split('$'), out)))

    d = {}
    for i in l:
        if i['LVM2_LV_ATTR'][0] == 't':
            k = "%s/%s" % (i['LVM2_VG_NAME'], i['LVM2_LV_NAME'])
        else:
            k = os.path.realpath(i['LVM2_LV_PATH'])
        d.update({k: i})
    return d


def getMountStats(exclude, include):
    def _getMounts(exclude=[], include=[]):
        excludeList = map(_getMountPoint, exclude)
        includeList = map(_getMountPoint, include)
        mountPoints = _parseProcMounts()
        if excludeList:
            outList = set(mountPoints) - set(excludeList)
        elif includeList:
            outList = set(mountPoints).intersection(set(includeList))
        else:
            return mountPoints
        # list comprehension to build dictionary does not work in python 2.6.6
        mounts = {}
        for k in outList:
            mounts[k] = mountPoints[k]
        return mounts

    def _getThinPoolStat(device):
        out = {'thinpool_size': None,
               'thinpool_used_percent': None,
               'metadata_size': None,
               'metadata_used_percent': None,
               'thinpool_free': None,
               'metadata_free': None,
               'thinpool_used': None,
               'metadata_used': None}

        if lvs and device in lvs and \
           lvs[device]['LVM2_LV_ATTR'][0] == 'V':
            thinpool = "%s/%s" % (lvs[device]['LVM2_VG_NAME'],
                                  lvs[device]['LVM2_POOL_LV'])
            out['thinpool_size'] = float(
                lvs[thinpool]['LVM2_LV_SIZE']) / 1024
            out['thinpool_used_percent'] = float(
                lvs[thinpool]['LVM2_DATA_PERCENT'])
            out['metadata_size'] = float(
                lvs[thinpool]['LVM2_LV_METADATA_SIZE']) / 1024
            out['metadata_used_percent'] = float(
                lvs[thinpool]['LVM2_METADATA_PERCENT'])
            out['thinpool_free'] = out['thinpool_size'] * (
                1 - out['thinpool_used_percent'] / 100.0)
            out['thinpool_used'] = out['thinpool_size'] - out['thinpool_free']
            out['metadata_free'] = out['metadata_size'] * (
                1 - out['metadata_used_percent'] / 100.0)
            out['metadata_used'] = out['metadata_size'] - out['metadata_free']
        return out

    mountPoints = _getMounts(exclude, include)
    lvs = getLvs()
    mountDetail = {}
    for mount, info in mountPoints.iteritems():
        mountDetail[mount] = _getStats(mount)
        mountDetail[mount].update(
            _getThinPoolStat(os.path.realpath(info['device']))
        )
        mountDetail[mount].update({'mount_point': mount})
    return mountDetail


def getPrintableStatus(mountDetail, warning, critical):
    finalRc = utils.PluginStatus.OK
    finalMsg = []
    finalOut = []
    finalPerfdata = []
    for mount, detail in mountDetail.iteritems():
        finalOut.append(_getOutputText(detail))
        finalPerfdata.append(_getPerfdata(detail, warning, critical))
        rc, msg = _getStatusInfo(detail, warning, critical)
        if msg:
            finalMsg.append(msg)
        if getattr(PluginStatusCode, rc) > getattr(PluginStatusCode, finalRc):
            finalRc = rc
    return finalRc, finalMsg, finalOut, finalPerfdata


if __name__ == '__main__':
    args = parse_input()
    mountDetail = getMountStats(args.exclude, args.include)
    rc, msg, msgDet, perfdata = getPrintableStatus(mountDetail,
                                                   args.warning,
                                                   args.critical)

    print "%s: %s" % (rc, ", ".join(msg))
    print "%s | %s" % ("\n".join(msgDet), "\n".join(perfdata))
    sys.exit(getattr(PluginStatusCode, rc))
