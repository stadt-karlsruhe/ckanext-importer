#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) 2018 Stadt Karlsruhe (www.karlsruhe.de)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# See conftest.py for the definition of the pytest fixtures


def test_package_creation(api, imp):
    '''
    Test creation of a package.
    '''
    title = 'Hello, world!'
    with imp.sync_package('x') as pkg:
        # Check that the package already exists
        id = pkg['id']
        api.action.package_show(id=id)

        pkg['title'] = title

    # Check that the changes have been synced
    assert api.action.package_show(id=id)['title'] == title


def test_package_update(api, imp):
    '''
    Test update of a package.
    '''
    for i in range(3):
        title = str(i)
        with imp.sync_package('y') as pkg:
            pkg['title'] = title
            id = pkg['id']
        assert api.action.package_show(id=id)['title'] == title


def test_different_importer_ids_same_package_eid(imp_factory):
    '''
    Test that different importers can use the same package EID.
    '''
    imp1 = imp_factory(id='test-importer-1')
    imp2 = imp_factory(id='test-importer-2')
    eid = 'shared-eid'
    with imp1.sync_package(eid) as pkg:
        pkg_id1 = pkg['id']
    with imp2.sync_package(eid) as pkg:
        pkg_id2 = pkg['id']
    assert pkg_id1 != pkg_id2
