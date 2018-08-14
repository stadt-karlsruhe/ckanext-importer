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

import StringIO

import pytest

from ckanext.importer import Importer, ExtrasDictView
import ckanapi


# See conftest.py for the definition of the pytest fixtures


class TestImporter(object):

    def test_package_creation(self, imp, api):
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

    def test_nonstring_importer_id(self, api):
        '''
        Test that non-string importer IDs are converted to strings.
        '''
        ids = [
            1,
            True,
            None,
            {'foo': 'bar'},
            ['foo', 'bar'],
        ]
        for id in ids:
            imp = Importer(id)
            assert isinstance(imp.id, unicode)

    def test_nonstring_package_eids(self, imp):
        '''
        Test that package EIDs don't have to be strings.
        '''
        eids = [
            1,
            True,
            None,
            {'foo': 'bar'},
            ['foo', 'bar'],
        ]
        ids = set()
        for eid in eids:
            with imp.sync_package(eid) as pkg:
                ids.add(pkg['id'])
        assert len(ids) == len(eids)

    def test_package_creation_default_names(self, imp):
        '''
        Test default name generation for multiple packages.
        '''
        names = set()
        n = 3
        for i in range(n):
            with imp.sync_package(i) as pkg:
                names.add(pkg['name'])
        assert len(names) == n

    def test_multiple_packages_same_importer_id_same_eid(self, api, imp):
        '''
        Test multiple packages with the same import ID and EID.
        '''
        eid = 'b'
        extras = [{'key': 'ckanext_importer_importer_id', 'value': imp.id},
                  {'key': 'ckanext_importer_package_eid', 'value': eid}]
        pkg1 = api.action.package_create(name='pkg1', extras=extras)
        pkg2 = api.action.package_create(name='pkg2', extras=extras)
        with pytest.raises(ValueError):
            with imp.sync_package(eid) as pkg:
                pass

    def test_package_update(self, api, imp):
        '''
        Test update of a package.
        '''
        for i in range(3):
            title = str(i)
            with imp.sync_package('y') as pkg:
                pkg['title'] = title
                id = pkg['id']
            assert api.action.package_show(id=id)['title'] == title

    def test_different_importer_ids_same_package_eid(self, imp_factory):
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

    def test_repr(self, imp):
        assert repr(imp) == '<Importer "{}">'.format(imp.id)


class TestPackage(object):

    def test_nonstring_resource_eids(self, pkg):
        '''
        Test that resource EIDs don't have to be strings.
        '''
        eids = [
            1,
            True,
            None,
            {'foo': 'bar'},
            ['foo', 'bar'],
        ]
        ids = set()
        for eid in eids:
            with pkg.sync_resource(eid) as res:
                ids.add(res['id'])
        assert len(ids) == len(eids)

    def test_resource_creation(self, api, pkg):
        '''
        Test creation of a resource.
        '''
        name = 'Hello, world!'
        with pkg.sync_resource('x') as res:
            # Check that the resource already exists
            id = res['id']
            res_dict = api.action.resource_show(id=id)

            # Check that the cached package dict has been updated
            assert pkg['resources'][0] == res_dict

            res['name'] = name

        # Check that the changes have been uploaded
        assert api.action.resource_show(id=id)['name'] == name

        # Check that the cached package dict has been updated
        assert pkg['resources'][0]['name'] == name

    def test_multiple_resources_same_eid(self, api, imp):
        '''
        Test multiple resources with the same EID.
        '''
        pkg_eid = 'a'
        res_eid = 'b'
        with imp.sync_package(pkg_eid) as pkg:
            pass
        for _ in [1, 2]:
            api.action.resource_create(package_id=pkg['id'], url='foo',
                                       ckanext_importer_resource_eid=res_eid)
        with imp.sync_package(pkg_eid) as pkg:
            with pytest.raises(ValueError):
                with pkg.sync_resource(res_eid) as res:
                    pass

    def test_resource_update(self, api, pkg):
        '''
        Test update of a resource.
        '''
        for i in range(3):
            name = str(i)
            with pkg.sync_resource('y') as res:
                res['name'] = name
                id = res['id']
            # Check that the changes have been uploaded
            assert api.action.resource_show(id=id)['name'] == name

            # Check that the cached package dict has been updated
            assert pkg['resources'][0]['name'] == name

    def test_file_upload_during_resource_creation(self, api, pkg):
        '''
        Test uploading a file during resource creation.
        '''
        fake_file = StringIO.StringIO('1,2,3\r\n4,5,6')
        fake_file.name = 'fake.csv'
        with pkg.sync_resource('x') as res:
            res['upload'] = fake_file
        url = api.action.resource_show(id=res['id'])['url']
        assert url.endswith(fake_file.name)

        # Check that the `upload` key has not been stored in the cached
        # package dict
        assert 'upload' not in pkg['resources'][0]

    def test_file_upload_during_resource_update(self, api, pkg):
        '''
        Test uploading a file during resource creation.
        '''
        with pkg.sync_resource('x') as res:
            res['url'] = 'foo'

        fake_file = StringIO.StringIO('1,2,3\r\n4,5,6')
        fake_file.name = 'fake.csv'
        with pkg.sync_resource('x') as res:
            res['upload'] = fake_file
        url = api.action.resource_show(id=res['id'])['url']
        assert url.endswith(fake_file.name)

        # Check that the `upload` key has not been stored in the cached
        # package dict
        assert 'upload' not in pkg['resources'][0]

    def test_repr(self, pkg):
        assert repr(pkg) == '<Package {}>'.format(pkg['id'])


class TestResource(object):

    def test_nonstring_view_eids(self, res):
        '''
        Test that view EIDs don't have to be strings.
        '''
        eids = [
            1,
            True,
            None,
            {'foo': 'bar'},
            ['foo', 'bar'],
        ]
        ids = set()
        for eid in eids:
            with res.sync_view(eid) as view:
                view['title'] = 'title'
                view['view_type'] = 'text_view'
            ids.add(view['id'])
        assert len(ids) == len(eids)

    def test_view_creation(self, api, res):
        '''
        Test creation of a view.
        '''
        title = 'Hello, world!'
        with res.sync_view('x') as view:
            assert view == {}
            view['title'] = title
            view['view_type'] = 'text_view'
        assert api.action.resource_view_show(id=view['id'])['title'] == title

    def test_view_update(self, api, res):
        '''
        Test update of a view.
        '''
        eid = 'y'
        with res.sync_view(eid) as view:
            view['title'] = 'title'
            view['view_type'] = 'text_view'
        id = view['id']
        for i in range(3):
            title = str(i)
            with res.sync_view(eid) as view:
                view['title'] = title
            assert api.action.resource_view_show(id=id)['title'] == title

    def test_repr(self, res):
        assert repr(res) == '<Resource {}>'.format(res['id'])


class TestExtrasDictView(object):

    def test_getitem(self):
        extras = [{'key': 'a', 'value': 1}, {'key': 'b', 'value': 2}]
        view = ExtrasDictView(extras)
        assert view['a'] == 1
        assert view['b'] == 2
        with pytest.raises(KeyError):
            view['c']

    def test_setitem(self):
        extras = [{'key': 'a', 'value': 1}]
        view = ExtrasDictView(extras)
        view['b'] = 2
        assert extras[1] == {'key': 'b', 'value': 2}
        view['a'] = 3
        assert extras[0] == {'key': 'a', 'value': 3}

    def test_delitem(self):
        extras = [{'key': 'a', 'value': 1}, {'key': 'b', 'value': 2}]
        view = ExtrasDictView(extras)
        del view['a']
        assert len(extras) == 1
        assert extras[0]['key'] == 'b'
        with pytest.raises(KeyError):
            del view['c']
        del view['b']
        assert not extras

    def test_len(self):
        assert len(ExtrasDictView([])) == 0
        assert len(ExtrasDictView([{'key': 'a', 'value': 1}])) == 1
        assert len(ExtrasDictView([{'key': 'a', 'value': 1},
                                   {'key': 'b', 'value': 2}])) == 2

    def test_iter(self):
        assert list(ExtrasDictView([])) == []
        assert list(ExtrasDictView([{'key': 'a', 'value': 1}])) == ['a']
        assert list(ExtrasDictView([{'key': 'a', 'value': 1},
                                    {'key': 'b', 'value': 2}])) == ['a', 'b']
