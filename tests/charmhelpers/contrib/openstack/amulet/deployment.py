# Copyright 2014-2015 Canonical Limited.
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

import logging
import re
import sys

import six
from charmhelpers.contrib.amulet.deployment import (
    AmuletDeployment
)

DEBUG = logging.DEBUG
ERROR = logging.ERROR


class OpenStackAmuletDeployment(AmuletDeployment):
    """OpenStack amulet deployment.

       This class inherits from AmuletDeployment and has additional support
       that is specifically for use by OpenStack charms.
       """

    def __init__(self, series=None, openstack=None, source=None,
                 stable=True, log_level=DEBUG):
        """Initialize the deployment environment."""
        super(OpenStackAmuletDeployment, self).__init__(series)
        self.log = self.get_logger(level=log_level)
        self.log.info('OpenStackAmuletDeployment:  init')
        self.openstack = openstack
        self.source = source
        self.stable = stable

    def get_logger(self, name="deployment-logger", level=logging.DEBUG):
        """Get a logger object that will log to stdout."""
        log = logging
        logger = log.getLogger(name)
        fmt = log.Formatter("%(asctime)s %(funcName)s "
                            "%(levelname)s: %(message)s")

        handler = log.StreamHandler(stream=sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(fmt)

        logger.addHandler(handler)
        logger.setLevel(level)

        return logger

    def _determine_branch_locations(self, other_services):
        """Determine the branch locations for the other services.

           Determine if the local branch being tested is derived from its
           stable or next (dev) branch, and based on this, use the corresonding
           stable or next branches for the other_services."""

        self.log.info('OpenStackAmuletDeployment:  determine branch locations')

        # Charms outside the ~openstack-charmers
        base_charms = {
            'mysql': ['trusty'],
            'mongodb': ['trusty'],
            'nrpe': ['trusty', 'xenial'],
        }

        for svc in other_services:
            # If a location has been explicitly set, use it
            if svc.get('location'):
                continue
            if svc['name'] in base_charms:
                # NOTE: not all charms have support for all series we
                #       want/need to test against, so fix to most recent
                #       that each base charm supports
                target_series = self.series
                if self.series not in base_charms[svc['name']]:
                    target_series = base_charms[svc['name']][-1]
                svc['location'] = 'cs:{}/{}'.format(target_series,
                                                    svc['name'])
            elif self.stable:
                svc['location'] = 'cs:{}/{}'.format(self.series,
                                                    svc['name'])
            else:
                svc['location'] = 'cs:~openstack-charmers-next/{}/{}'.format(
                    self.series,
                    svc['name']
                )

        return other_services

    def _add_services(self, this_service, other_services, storage,
                      use_source=None,
                      no_origin=None):
        """Add services to the deployment and optionally set
        openstack-origin/source.

        :param this_service dict: Service dictionary describing the service
                                  whose amulet tests are being run
        :param other_services dict: List of service dictionaries describing
                                    the services needed to support the target
                                    service
        :param use_source list: List of services which use the 'source' config
                                option rather than 'openstack-origin'
        :param no_origin list: List of services which do not support setting
                               the Cloud Archive.
        Service Dict:
            {
                'name': str charm-name,
                'units': int number of units,
                'constraints': dict of juju constraints,
                'location': str location of charm,
            }
        eg
        this_service = {
            'name': 'openvswitch-odl',
            'constraints': {'mem': '8G'},
        }
        other_services = [
            {
                'name': 'nova-compute',
                'units': 2,
                'constraints': {'mem': '4G'},
                'location': cs:~bob/xenial/nova-compute
            },
            {
                'name': 'mysql',
                'constraints': {'mem': '2G'},
            },
            {'neutron-api-odl'}]
        use_source = ['mysql']
        no_origin = ['neutron-api-odl']
        """
        self.log.info('OpenStackAmuletDeployment:  adding services')

        other_services = self._determine_branch_locations(other_services)

        super(OpenStackAmuletDeployment, self)._add_services(
            this_service=this_service,
            other_services=other_services,
            storage=storage)

        services = other_services
        services.append(this_service)

        use_source = use_source or []
        no_origin = no_origin or []

        # Charms which should use the source config option
        use_source = list(set(
            use_source + ['mysql', 'mongodb', 'rabbitmq-server', 'ceph',
                          'ceph-osd', 'ceph-radosgw', 'ceph-mon',
                          'ceph-proxy', 'percona-cluster', 'lxd']))

        # Charms which can not use openstack-origin, ie. many subordinates
        no_origin = list(set(
            no_origin + ['cinder-ceph', 'hacluster', 'neutron-openvswitch',
                         'nrpe', 'openvswitch-odl', 'neutron-api-odl',
                         'odl-controller', 'cinder-backup', 'nexentaedge-data',
                         'nexentaedge-iscsi-gw', 'nexentaedge-swift-gw',
                         'cinder-nexentaedge', 'nexentaedge-mgmt']))

        if self.openstack:
            for svc in services:
                if svc['name'] not in use_source + no_origin:
                    config = {'openstack-origin': self.openstack}
                    self.d.configure(svc['name'], config)

        if self.source:
            for svc in services:
                if svc['name'] in use_source and svc['name'] not in no_origin:
                    config = {'source': self.source}
                    self.d.configure(svc['name'], config)

    def _configure_services(self, configs):
        """Configure all of the services."""
        self.log.info('OpenStackAmuletDeployment:  configure services')
        for service, config in six.iteritems(configs):
            self.d.configure(service, config)

    def _auto_wait_for_status(self, message=None, exclude_services=None,
                              include_only=None, timeout=1800):
        """Wait for all units to have a specific extended status, except
        for any defined as excluded.  Unless specified via message, any
        status containing any case of 'ready' will be considered a match.

        Examples of message usage:

          Wait for all unit status to CONTAIN any case of 'ready' or 'ok':
              message = re.compile('.*ready.*|.*ok.*', re.IGNORECASE)

          Wait for all units to reach this status (exact match):
              message = re.compile('^Unit is ready and clustered$')

          Wait for all units to reach any one of these (exact match):
              message = re.compile('Unit is ready|OK|Ready')

          Wait for at least one unit to reach this status (exact match):
              message = {'ready'}

        See Amulet's sentry.wait_for_messages() for message usage detail.
        https://github.com/juju/amulet/blob/master/amulet/sentry.py

        :param message: Expected status match
        :param exclude_services: List of juju service names to ignore,
            not to be used in conjuction with include_only.
        :param include_only: List of juju service names to exclusively check,
            not to be used in conjuction with exclude_services.
        :param timeout: Maximum time in seconds to wait for status match
        :returns: None.  Raises if timeout is hit.
        """
        self.log.info('Waiting for extended status on units...')

        all_services = self.d.services.keys()

        if exclude_services and include_only:
            raise ValueError('exclude_services can not be used '
                             'with include_only')

        if message:
            if isinstance(message, re._pattern_type):
                match = message.pattern
            else:
                match = message

            self.log.debug('Custom extended status wait match: '
                           '{}'.format(match))
        else:
            self.log.debug('Default extended status wait match:  contains '
                           'READY (case-insensitive)')
            message = re.compile('.*ready.*', re.IGNORECASE)

        if exclude_services:
            self.log.debug('Excluding services from extended status match: '
                           '{}'.format(exclude_services))
        else:
            exclude_services = []

        if include_only:
            services = include_only
        else:
            services = list(set(all_services) - set(exclude_services))

        self.log.debug('Waiting up to {}s for extended status on services: '
                       '{}'.format(timeout, services))
        service_messages = {service: message for service in services}
        self.d.sentry.wait_for_messages(service_messages, timeout=timeout)
        self.log.info('OK')
