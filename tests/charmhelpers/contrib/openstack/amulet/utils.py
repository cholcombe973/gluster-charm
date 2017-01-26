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

import six
from charmhelpers.contrib.amulet.utils import (
    AmuletUtils
)

DEBUG = logging.DEBUG
ERROR = logging.ERROR

NOVA_CLIENT_VERSION = "2"


class OpenStackAmuletUtils(AmuletUtils):
    """OpenStack amulet utilities.

       This class inherits from AmuletUtils and has additional support
       that is specifically for use by OpenStack charm tests.
       """

    def __init__(self, log_level=ERROR):
        """Initialize the deployment environment."""
        super(OpenStackAmuletUtils, self).__init__(log_level)

    def validate_endpoint_data(self, endpoints, admin_port, internal_port,
                               public_port, expected):
        """Validate endpoint data.

           Validate actual endpoint data vs expected endpoint data. The ports
           are used to find the matching endpoint.
           """
        self.log.debug('Validating endpoint data...')
        self.log.debug('actual: {}'.format(repr(endpoints)))
        found = False
        for ep in endpoints:
            self.log.debug('endpoint: {}'.format(repr(ep)))
            if (admin_port in ep.adminurl and
                    internal_port in ep.internalurl and
                    public_port in ep.publicurl):
                found = True
                actual = {'id': ep.id,
                          'region': ep.region,
                          'adminurl': ep.adminurl,
                          'internalurl': ep.internalurl,
                          'publicurl': ep.publicurl,
                          'service_id': ep.service_id}
                ret = self._validate_dict_data(expected, actual)
                if ret:
                    return 'unexpected endpoint data - {}'.format(ret)

        if not found:
            return 'endpoint not found'

    def validate_v3_endpoint_data(self, endpoints, admin_port, internal_port,
                                  public_port, expected):
        """Validate keystone v3 endpoint data.

        Validate the v3 endpoint data which has changed from v2.  The
        ports are used to find the matching endpoint.

        The new v3 endpoint data looks like:

        [<Endpoint enabled=True,
                   id=0432655fc2f74d1e9fa17bdaa6f6e60b,
                   interface=admin,
                   links={u'self': u'<RESTful URL of this endpoint>'},
                   region=RegionOne,
                   region_id=RegionOne,
                   service_id=17f842a0dc084b928e476fafe67e4095,
                   url=http://10.5.6.5:9312>,
         <Endpoint enabled=True,
                   id=6536cb6cb92f4f41bf22b079935c7707,
                   interface=admin,
                   links={u'self': u'<RESTful url of this endpoint>'},
                   region=RegionOne,
                   region_id=RegionOne,
                   service_id=72fc8736fb41435e8b3584205bb2cfa3,
                   url=http://10.5.6.6:35357/v3>,
                   ... ]
        """
        self.log.debug('Validating v3 endpoint data...')
        self.log.debug('actual: {}'.format(repr(endpoints)))
        found = []
        for ep in endpoints:
            self.log.debug('endpoint: {}'.format(repr(ep)))
            if ((admin_port in ep.url and ep.interface == 'admin') or
                    (internal_port in ep.url and ep.interface == 'internal') or
                    (public_port in ep.url and ep.interface == 'public')):
                found.append(ep.interface)
                # note we ignore the links member.
                actual = {'id': ep.id,
                          'region': ep.region,
                          'region_id': ep.region_id,
                          'interface': self.not_null,
                          'url': ep.url,
                          'service_id': ep.service_id, }
                ret = self._validate_dict_data(expected, actual)
                if ret:
                    return 'unexpected endpoint data - {}'.format(ret)

        if len(found) != 3:
            return 'Unexpected number of endpoints found'

    def validate_svc_catalog_endpoint_data(self, expected, actual):
        """Validate service catalog endpoint data.

           Validate a list of actual service catalog endpoints vs a list of
           expected service catalog endpoints.
           """
        self.log.debug('Validating service catalog endpoint data...')
        self.log.debug('actual: {}'.format(repr(actual)))
        for k, v in six.iteritems(expected):
            if k in actual:
                ret = self._validate_dict_data(expected[k][0], actual[k][0])
                if ret:
                    return self.endpoint_error(k, ret)
            else:
                return "endpoint {} does not exist".format(k)
        return ret

    def validate_v3_svc_catalog_endpoint_data(self, expected, actual):
        """Validate the keystone v3 catalog endpoint data.

        Validate a list of dictinaries that make up the keystone v3 service
        catalogue.

        It is in the form of:


        {u'identity': [{u'id': u'48346b01c6804b298cdd7349aadb732e',
                        u'interface': u'admin',
                        u'region': u'RegionOne',
                        u'region_id': u'RegionOne',
                        u'url': u'http://10.5.5.224:35357/v3'},
                       {u'id': u'8414f7352a4b47a69fddd9dbd2aef5cf',
                        u'interface': u'public',
                        u'region': u'RegionOne',
                        u'region_id': u'RegionOne',
                        u'url': u'http://10.5.5.224:5000/v3'},
                       {u'id': u'd5ca31440cc24ee1bf625e2996fb6a5b',
                        u'interface': u'internal',
                        u'region': u'RegionOne',
                        u'region_id': u'RegionOne',
                        u'url': u'http://10.5.5.224:5000/v3'}],
         u'key-manager': [{u'id': u'68ebc17df0b045fcb8a8a433ebea9e62',
                           u'interface': u'public',
                           u'region': u'RegionOne',
                           u'region_id': u'RegionOne',
                           u'url': u'http://10.5.5.223:9311'},
                          {u'id': u'9cdfe2a893c34afd8f504eb218cd2f9d',
                           u'interface': u'internal',
                           u'region': u'RegionOne',
                           u'region_id': u'RegionOne',
                           u'url': u'http://10.5.5.223:9311'},
                          {u'id': u'f629388955bc407f8b11d8b7ca168086',
                           u'interface': u'admin',
                           u'region': u'RegionOne',
                           u'region_id': u'RegionOne',
                           u'url': u'http://10.5.5.223:9312'}]}

        Note, that an added complication is that the order of admin, public,
        internal against 'interface' in each region.

        Thus, the function sorts the expected and actual lists using the
        interface key as a sort key, prior to the comparison.
        """
        self.log.debug('Validating v3 service catalog endpoint data...')
        self.log.debug('actual: {}'.format(repr(actual)))
        for k, v in six.iteritems(expected):
            if k in actual:
                l_expected = sorted(v, key=lambda x: x['interface'])
                l_actual = sorted(actual[k], key=lambda x: x['interface'])
                if len(l_actual) != len(l_expected):
                    return ("endpoint {} has differing number of interfaces "
                            " - expected({}), actual({})"
                            .format(k, len(l_expected), len(l_actual)))
                for i_expected, i_actual in zip(l_expected, l_actual):
                    self.log.debug("checking interface {}"
                                   .format(i_expected['interface']))
                    ret = self._validate_dict_data(i_expected, i_actual)
                    if ret:
                        return self.endpoint_error(k, ret)
            else:
                return "endpoint {} does not exist".format(k)
        return ret

    def validate_tenant_data(self, expected, actual):
        """Validate tenant data.

           Validate a list of actual tenant data vs list of expected tenant
           data.
           """
        self.log.debug('Validating tenant data...')
        self.log.debug('actual: {}'.format(repr(actual)))
        for e in expected:
            found = False
            for act in actual:
                a = {'enabled': act.enabled, 'description': act.description,
                     'name': act.name, 'id': act.id}
                if e['name'] == a['name']:
                    found = True
                    ret = self._validate_dict_data(e, a)
                    if ret:
                        return "unexpected tenant data - {}".format(ret)
            if not found:
                return "tenant {} does not exist".format(e['name'])
        return ret

    def validate_role_data(self, expected, actual):
        """Validate role data.

           Validate a list of actual role data vs a list of expected role
           data.
           """
        self.log.debug('Validating role data...')
        self.log.debug('actual: {}'.format(repr(actual)))
        for e in expected:
            found = False
            for act in actual:
                a = {'name': act.name, 'id': act.id}
                if e['name'] == a['name']:
                    found = True
                    ret = self._validate_dict_data(e, a)
                    if ret:
                        return "unexpected role data - {}".format(ret)
            if not found:
                return "role {} does not exist".format(e['name'])
        return ret

    def validate_user_data(self, expected, actual, api_version=None):
        """Validate user data.

           Validate a list of actual user data vs a list of expected user
           data.
           """
        self.log.debug('Validating user data...')
        self.log.debug('actual: {}'.format(repr(actual)))
        for e in expected:
            found = False
            for act in actual:
                if e['name'] == act.name:
                    a = {'enabled': act.enabled, 'name': act.name,
                         'email': act.email, 'id': act.id}
                    if api_version == 3:
                        a['default_project_id'] = getattr(act,
                                                          'default_project_id',
                                                          'none')
                    else:
                        a['tenantId'] = act.tenantId
                    found = True
                    ret = self._validate_dict_data(e, a)
                    if ret:
                        return "unexpected user data - {}".format(ret)
            if not found:
                return "user {} does not exist".format(e['name'])
        return ret

