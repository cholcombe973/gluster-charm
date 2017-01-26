#!/usr/bin/env python3
#
# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import amulet

from charmhelpers.contrib.openstack.amulet.deployment import (
    OpenStackAmuletDeployment
)
from charmhelpers.contrib.openstack.amulet.utils import (  # noqa
    OpenStackAmuletUtils,
    DEBUG,
    )

# Use DEBUG to turn on debug logging
u = OpenStackAmuletUtils(DEBUG)


class GlusterFsBasicDeployment(OpenStackAmuletDeployment):
    """Amulet tests on a basic ceph deployment."""

    def __init__(self, series=None, source=None, stable=False):
        """Deploy the entire test environment."""
        super(GlusterFsBasicDeployment, self).__init__(series=series,
                                                       source=source,
                                                       stable=stable)
        self._add_services()
        self._add_relations()
        self._configure_services()
        self._deploy()

        u.log.info('Waiting on extended status checks...')
        exclude_services = []

        # Wait for deployment ready msgs, except exclusions
        self._auto_wait_for_status(exclude_services=exclude_services)

        self.d.sentry.wait()
        self._initialize_tests()

    def _add_services(self, **kwargs):
        """Add services

           Add the services that we're testing, where gluster is local,
           and the rest of the service are from lp branches that are
           compatible with the local charm (e.g. stable or next).
        :param **kwargs:
           """
        this_service = {'name': 'gluster', 'units': 3}
        super(GlusterFsBasicDeployment, self)._add_services(
            this_service=this_service, other_services=[],
            storage={'brick': 'ebs,10g,1'})

    def _add_relations(self, **kwargs):
        """Add all of the relations for the services.
        :param **kwargs:
        """
        pass
        # relations = {
        #    'ceph-osd:mon': 'ceph-mon:osd',
        #    'ceph-fs:ceph-mds': 'ceph-mon:mds',
        # }
        # super(GlusterFsBasicDeployment, self)._add_relations(relations)

    def _configure_services(self, **kwargs):
        """Configure all of the services.
        :param **kwargs:
        """
        gluster_config = {
            'volume_name': 'amulet',
            'cluster_type': 'DistributedAndReplicate',
            'replication_level': '3',
        }

        configs = {
            'gluster': gluster_config,
        }
        super(GlusterFsBasicDeployment, self)._configure_services(configs)

    def _initialize_tests(self):
        """Perform final initialization before tests get run."""
        # Access the sentries for inspecting service units
        self.gluster0_sentry = self.d.sentry['gluster'][0]
        self.gluster1_sentry = self.d.sentry['gluster'][1]
        self.gluster2_sentry = self.d.sentry['gluster'][2]

    def test_100_gluster_processes(self):
        """Verify that the expected service processes are running
        on each ceph unit."""

        # Process name and quantity of processes to expect on each unit
        gluster_processes = {
            'glusterfsd': 1,
            'glusterd': 1,
        }

        # Units with process names and PID quantities expected
        expected_processes = {
            self.gluster0_sentry: gluster_processes,
            self.gluster1_sentry: gluster_processes,
            self.gluster2_sentry: gluster_processes,
        }

        actual_pids = u.get_unit_process_ids(expected_processes)
        ret = u.validate_unit_process_ids(expected_processes, actual_pids)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)

    def get_gluster_volumes(self, sentry_unit):
        """Return a list of ceph pools from a single ceph unit, with
        pool name as keys, pool id as vals."""
        volumes = []
        cmd = 'sudo gluster vol info'
        output, code = sentry_unit.run(cmd)
        if code != 0:
            msg = ('{} `{}` returned {} '
                   '{}'.format(sentry_unit.info['unit_name'],
                               cmd, code, output))
            amulet.raise_status(amulet.FAIL, msg=msg)

        for volume_part in str(output).splitlines():
            if len(volume_part.strip()) == 0:
                # Skip empty lines
                continue
            key_value = volume_part.split(': ')
            if len(key_value) == 2:
                if key_value[0] == "Volume Name":
                    volumes.append(key_value[1])

        self.log.debug('Volumes on {}: {}'.format(sentry_unit.info['unit_name'],
                                                  volumes))
        return volumes

    def test_200_gluster_check_volumes(self):
        """Check volume on all gluster units, expect them to be
        identical"""
        u.log.debug('Checking volumes on gluster units...')
        expected_volumes = ['amulet']
        results = []
        sentries = [
            self.gluster0_sentry,
            self.gluster1_sentry,
            self.gluster2_sentry
        ]

        # Check for presence of expected pools on each unit
        u.log.debug('Expected volumes: {}'.format(expected_volumes))
        for sentry_unit in sentries:
            volumes = self.get_gluster_volumes(sentry_unit)
            results.extend(volumes)

            for expected_volume in expected_volumes:
                if expected_volume not in volumes:
                    msg = ('{} does not have volume: '
                           '{}'.format(sentry_unit.info['unit_name'],
                                       expected_volume))
                    amulet.raise_status(amulet.FAIL, msg=msg)
            u.log.debug('{} has (at least) the expected '
                        'volumes.'.format(sentry_unit.info['unit_name']))

        # Check that all units returned the same pool name:id data
        ret = len(set(results))
        if ret is not 1:
            u.log.debug('Volume list results: {}'.format(results))
            msg = ('{}; Volume list results are not identical on all '
                   'gluster units.'.format(ret))
            amulet.raise_status(amulet.FAIL, msg=msg)
        else:
            u.log.debug('Volume list on all gluster units produced the '
                        'same results (OK).')

    def test_499_gluster_cmds_exit_zero(self):
        """Check basic functionality of gluster cli commands against
        all gluster units."""
        sentry_units = [
            self.gluster0_sentry,
            self.gluster1_sentry,
            self.gluster2_sentry
        ]
        commands = [
            'sudo gluster vol status amulet',  # get the status of our volume
        ]
        ret = u.check_commands_on_units(commands, sentry_units)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)

            # FYI: No restart check as gluster services do not restart
            # when charm config changes
