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

import logging
import StringIO

import pytest
import mock

import ckanapi
import ckan.logic
from ckan.tests.factories import Organization

from ckanext.importer import Entity, ExtrasDictView, Importer, OnError


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

    def test_default_names_generation_during_package_creation(self, imp):
        '''
        Test default name generation for multiple packages.
        '''
        names = set()
        n = 3
        for i in range(n):
            with imp.sync_package(i) as pkg:
                names.add(pkg['name'])
        assert len(names) == n

    def test_multiple_packages_with_the_same_importer_id_and_the_same_eid(self, api, imp):
        '''
        Test multiple packages with the same import ID and EID.
        '''
        eid = 'b'
        extras = [{'key': 'ckanext_importer_importer_id', 'value': imp.id},
                  {'key': 'ckanext_importer_package_eid', 'value': eid}]
        pkg1 = api.action.package_create(name='pkg1', extras=extras)
        pkg2 = api.action.package_create(name='pkg2', extras=extras)
        with pytest.raises(RuntimeError):
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

    def test_unmodified_package_creation(self, imp):
        '''
        Test not modifying a new package.
        '''
        with mock.patch('ckanext.importer.Package._upload') as upload:
            with imp.sync_package('x'):
                pass
            upload.assert_not_called()

    def test_unmodified_package_update(self, imp):
        '''
        Test not modifying an existing package.
        '''
        with imp.sync_package('x') as pkg:
            pkg['title'] = 'Foobar'
        with mock.patch('ckanext.importer.Package._upload') as upload:
            with imp.sync_package('x'):
                pass
            upload.assert_not_called()

    def test_different_importer_ids_with_the_same_package_eid(self, imp_factory):
        '''
        Test that different importers can use the same package EID.
        '''
        eid = 'shared-eid'
        imp1 = imp_factory(id='test-importer-1')
        with imp1.sync_package(eid) as pkg:
            pkg_id1 = pkg['id']
        imp2 = imp_factory(id='test-importer-2')
        with imp2.sync_package(eid) as pkg:
            pkg_id2 = pkg['id']
        assert pkg_id1 != pkg_id2

    def test_repr(self, imp):
        assert repr(imp) == '<Importer id={!r}>'.format(imp.id)

    def test_delete_during_package_creation(self, api, imp):
        '''
        Test deletion of a new package.
        '''
        with imp.sync_package('y') as pkg:
            id_y = pkg['id']
        with imp.sync_package('x') as pkg:
            id_x = pkg['id']
            pkg.delete()
        with pytest.raises(ckanapi.NotFound):
            api.action.package_show(id=id_x)
        api.action.package_show(id=id_y)

    def test_delete_during_package_update(self, api, imp):
        '''
        Test deletion of an existing package.
        '''
        with imp.sync_package('y') as pkg:
            id_y = pkg['id']
        with imp.sync_package('x'):
            pass
        with imp.sync_package('x') as pkg:
            id_x = pkg['id']
            pkg.delete()
        with pytest.raises(ckanapi.NotFound):
            api.action.package_show(id=id_x)
        api.action.package_show(id=id_y)

    def test_delete_unsynced_packages(self, api, imp_factory):
        '''
        Test deletion of unsynced packages.
        '''
        importer_id = 'importer-id'
        eids = [unicode(i) for i in range(13)]
        ids = {}
        imp = imp_factory(importer_id)
        for eid in eids:
            with imp.sync_package(eid) as pkg:
                ids[eid] = pkg['id']
        imp = imp_factory(importer_id)
        with imp.sync_package(eids[1]) as pkg:
            # Sync with changes
            pkg['title'] = 'A new title'
        with imp.sync_package(eids[3]) as pkg:
            # Sync without any changes
            pass
        with pytest.raises(ValueError):
            with imp.sync_package(eids[5], on_error=OnError.reraise) as pkg:
                # Reraised error
                raise ValueError('Oops')
        with imp.sync_package(eids[7], on_error=OnError.keep) as pkg:
            # Swallowed error
            raise ValueError('Oops')
        with imp.sync_package(eids[9], on_error=OnError.delete) as pkg:
            # Swallowed error with entity deletion
            raise ValueError('Oops')
        with imp.sync_package(eids[11]) as pkg:
            # Delete
            pkg.delete()
        imp.delete_unsynced_packages()
        for eid in '0', '2', '4', '6', '8', '9', '10', '11', '12':
            with pytest.raises(ckanapi.NotFound):
                api.action.package_show(id=ids[eid])
        for eid in '1', '3', '5', '7':
            api.action.package_show(id=ids[eid])

    def test_find_packages(self, api, imp):
        def extras(eid):
            return [{'key': 'ckanext_importer_importer_id', 'value': imp.id},
                    {'key': 'ckanext_importer_package_eid', 'value': eid}]
        assert list(imp.find_packages()) == []
        id1 = api.action.package_create(name='p1', extras=extras('a'))['id']
        assert {p['id'] for p in imp.find_packages()} == {id1}
        assert {p['id'] for p in imp.find_packages('a')} == {id1}
        id2 = api.action.package_create(name='p2', extras=extras('b'))['id']
        assert {p['id'] for p in imp.find_packages()} == {id1, id2}
        assert {p['id'] for p in imp.find_packages('a')} == {id1}
        assert {p['id'] for p in imp.find_packages('b')} == {id2}
        id3 = api.action.package_create(name='p3', extras=extras('a'))['id']
        assert {p['id'] for p in imp.find_packages()} == {id1, id2, id3}
        assert {p['id'] for p in imp.find_packages('a')} == {id1, id3}
        assert {p['id'] for p in imp.find_packages('b')} == {id2}

    def test_find_package(self, api, imp):
        def extras(eid):
            return [{'key': 'ckanext_importer_importer_id', 'value': imp.id},
                    {'key': 'ckanext_importer_package_eid', 'value': eid}]
        with pytest.raises(ckan.logic.NotFound):
            imp.find_package('some-eid')
        id1 = api.action.package_create(name='p1', extras=extras('a'))['id']
        assert imp.find_package('a')['id'] == id1
        with pytest.raises(ckan.logic.NotFound):
            imp.find_package('some-eid')
        api.action.package_create(name='p2', extras=extras('a'))['id']
        with pytest.raises(RuntimeError):
            imp.find_package('a')

    def test_logging_prefix(self, imp, caplog):
        '''
        Test that log messages are prefixed with the importer ID.
        '''
        caplog.set_level(logging.DEBUG)
        imp._log.debug('x')
        imp._log.info('x')
        imp._log.warning('x')
        imp._log.error('x')
        imp._log.critical('x')
        try:
            raise ValueError('oops')
        except ValueError:
            imp._log.exception('x')
        for level in (logging.DEBUG, logging.INFO, logging.WARNING,
                      logging.ERROR, logging.CRITICAL):
            imp._log.log(level, 'x')
        assert len(caplog.records) == 11
        prefix = 'Importer {!r}: '.format(imp.id)
        for record in caplog.records:
            assert record.message.startswith(prefix)

    def test_default_owner_org_given(self, api, imp_factory):
        '''
        Test speciyfing the ``default_owner_org`` option.
        '''
        org_id = Organization()['id']
        imp = imp_factory(default_owner_org=org_id)
        with imp.sync_package('x') as pkg:
            assert pkg['owner_org'] == org_id
            assert api.action.package_show(id=pkg['id'])['owner_org'] == org_id

    def test_default_owner_org_not_given(self, api, imp_factory):
        '''
        Test not speciyfing the ``default_owner_org`` option.
        '''
        imp = imp_factory()
        with imp.sync_package('x') as pkg:
            assert not pkg['owner_org']
            assert not api.action.package_show(id=pkg['id'])['owner_org']

    def test_exception_during_package_creation_reraise(self, api, imp, caplog):
        '''
        Test error handling during package creation with on_error=reraise.
        '''
        with pytest.raises(ValueError):
            with imp.sync_package('x', on_error=OnError.reraise) as pkg:
                id = pkg['id']
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckan.logic.NotFound):
            api.action.package_show(id=id)

    def test_exception_during_package_creation_keep(self, api, imp, caplog):
        '''
        Test error handling during package creation with on_error=keep.
        '''
        with imp.sync_package('x', on_error=OnError.keep) as pkg:
            id = pkg['id']
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckan.logic.NotFound):
            api.action.package_show(id=id)

    def test_exception_during_package_creation_delete(self, api, imp, caplog):
        '''
        Test error handling during package creation with on_error=delete.
        '''
        with imp.sync_package('x', on_error=OnError.delete) as pkg:
            id = pkg['id']
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckan.logic.NotFound):
            api.action.package_show(id=id)

    def test_exception_during_package_update_reraise(self, api, imp, caplog):
        '''
        Test error handling during package update with on_error=reraise.
        '''
        eid = 'x'
        with imp.sync_package(eid) as pkg:
            id = pkg['id']
            old_title = pkg['title']
        with pytest.raises(ValueError):
            with imp.sync_package(eid, on_error=OnError.reraise) as pkg:
                pkg['title'] = 'A new title'
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.package_show(id=id)['title'] == old_title

    def test_exception_during_package_update_keep(self, api, imp, caplog):
        '''
        Test error handling during package update with on_error=keep.
        '''
        eid = 'x'
        with imp.sync_package(eid) as pkg:
            id = pkg['id']
            old_title = pkg['title']
        with imp.sync_package(eid, on_error=OnError.keep) as pkg:
            pkg['title'] = 'A new title'
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.package_show(id=id)['title'] == old_title

    def test_exception_during_package_update_delete(self, api, imp, caplog):
        '''
        Test error handling during package update with on_error=delete.
        '''
        eid = 'x'
        with imp.sync_package(eid) as pkg:
            id = pkg['id']
        with imp.sync_package(eid, on_error=OnError.delete) as pkg:
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckan.logic.NotFound):
            api.action.package_show(id=id)


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

    def test_multiple_resources_with_the_same_eid(self, api, imp):
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

    def test_unmodified_resource_creation(self, pkg):
        '''
        Test not modifying a new resource.
        '''
        with mock.patch('ckanext.importer.Resource._upload') as upload:
            with pkg.sync_resource('x'):
                pass
            upload.assert_not_called()

    def test_unmodified_resource_update(self, pkg):
        '''
        Test not modifying an existing resource.
        '''
        with pkg.sync_resource('x') as res:
            res['name'] = 'Foobar'
        with mock.patch('ckanext.importer.Resource._upload') as upload:
            with pkg.sync_resource('x'):
                pass
            upload.assert_not_called()

    def test_resource_creation_during_unmodified_package_creation(self, api, imp):
        '''
        Test not modifying a new package aside from creating a resource.

        Creating the resource changes the cached package dict but should
        not trigger a package upload on its own.
        '''
        with mock.patch('ckanext.importer.Package._upload') as upload:
            with imp.sync_package('x') as pkg:
                with pkg.sync_resource('y') as res:
                    id = res['id']
            upload.assert_not_called()
        api.action.resource_show(id=id)

    def test_resource_creation_during_unmodified_package_update(self, api, imp):
        '''
        Test not modifying an existing package aside from creating a resource.

        Creating the resource changes the cached package dict but should
        not trigger a package upload on its own.
        '''
        with imp.sync_package('x'):
            pass
        with mock.patch('ckanext.importer.Package._upload') as upload:
            with imp.sync_package('x') as pkg:
                with pkg.sync_resource('y') as res:
                    id = res['id']
            upload.assert_not_called()
        api.action.resource_show(id=id)

    def test_resource_update_during_unmodified_package_update(self, api, imp):
        '''
        Test not modifying an existing package aside from updating a resource.

        Updating the resource changes the cached package dict but should
        not trigger a package upload on its own.
        '''
        with imp.sync_package('x') as pkg:
            with pkg.sync_resource('y'):
                pass
        with mock.patch('ckanext.importer.Package._upload') as upload:
            with imp.sync_package('x') as pkg:
                with pkg.sync_resource('y') as res:
                    id = res['id']
                    res['name'] = 'foobar'
            upload.assert_not_called()
        assert api.action.resource_show(id=id)['name'] == 'foobar'

    def test_resource_creation_during_modified_package_creation(self, api, imp):
        '''
        Test modifying a new package before creating a resource.

        The code that prevents packages which are unmodified aside from
        the resource creation from being uploaded should not prevent the
        upload in this case.
        '''
        with imp.sync_package('x') as pkg:
            pkg['title'] = 'Title'
            with pkg.sync_resource('y'):
                pass
        assert api.action.package_show(id=pkg['id'])['title'] == 'Title'

    def test_resource_creation_during_modified_package_update(self, api, imp):
        '''
        Test modifying an existing package before creating a resource.

        The code that prevents packages which are unmodified aside from
        the resource creation from being uploaded should not prevent the
        upload in this case.
        '''
        with imp.sync_package('x'):
            pass
        with imp.sync_package('x') as pkg:
            pkg['title'] = 'Title'
            with pkg.sync_resource('y'):
                pass
        assert api.action.package_show(id=pkg['id'])['title'] == 'Title'

    def test_resource_update_during_modified_package_update(self, api, imp):
        '''
        Test modifying an existing package before updating a resource.

        The code that prevents packages which are unmodified aside from
        the resource creation from being uploaded should not prevent the
        upload in this case.
        '''
        with imp.sync_package('x') as pkg:
            with pkg.sync_resource('y'):
                pass
        with imp.sync_package('x') as pkg:
            pkg['title'] = 'Title'
            with pkg.sync_resource('y') as res:
                res['name'] = 'foobar'
        assert api.action.package_show(id=pkg['id'])['title'] == 'Title'

    def test_repr(self, pkg):
        assert repr(pkg) == '<Package id={!r} eid={!r}>'.format(pkg['id'], pkg._eid)

    def test_delete_during_resource_creation(self, api, pkg):
        '''
        Test deletion of a new resource.
        '''
        with pkg.sync_resource('a') as res:
            id_a = res['id']
        with pkg.sync_resource('b') as res:
            id_b = res['id']
            res.delete()
        with pytest.raises(ckanapi.NotFound):
            api.action.resource_show(id=id_b)
        api.action.resource_show(id=id_a)

    def test_delete_during_resource_update(self, api, pkg):
        '''
        Test deletion of an existing resource.
        '''
        with pkg.sync_resource('a') as res:
            id_a = res['id']
        with pkg.sync_resource('b') as res:
            pass
        with pkg.sync_resource('c') as res:
            id_c = res['id']
        with pkg.sync_resource('b') as res:
            res.delete()
        pkg_dict = api.action.package_show(id=pkg['id'])
        assert [res['id'] for res in pkg_dict['resources']] == [id_a, id_c]

    def test_delete_unsynced_resources(self, api, imp):
        '''
        Test deletion of unsynced resources.
        '''
        pkg_eid = 'x'
        res_eids = [unicode(i) for i in range(13)]
        res_ids = {}
        with imp.sync_package(pkg_eid) as pkg:
            for res_eid in res_eids:
                with pkg.sync_resource(res_eid) as res:
                    res_ids[res_eid] = res['id']
        with imp.sync_package(pkg_eid) as pkg:
            with pkg.sync_resource(res_eids[1]) as res:
                # Sync with changes
                res['name'] = 'A new name'
            with pkg.sync_resource(res_eids[3]) as res:
                # Sync without any changes
                pass
            with pytest.raises(ValueError):
                with pkg.sync_resource(res_eids[5], on_error=OnError.reraise) as res:
                    # Reraised error
                    raise ValueError('Oops')
            with pkg.sync_resource(res_eids[7], on_error=OnError.keep) as res:
                # Swallowed error
                raise ValueError('Oops')
            with pkg.sync_resource(res_eids[9], on_error=OnError.delete) as res:
                # Swallowed error with entity deletion
                raise ValueError('Oops')
            with pkg.sync_resource(res_eids[11]) as res:
                # Delete
                res.delete()
            pkg.delete_unsynced_resources()
        for res_eid in '0', '2', '4', '6', '8', '9', '10', '11', '12':
            with pytest.raises(ckanapi.NotFound):
                api.action.resource_show(id=res_ids[res_eid])
        for res_eid in '1', '3', '5', '7':
            api.action.resource_show(id=res_ids[res_eid])

    def test_exception_during_resource_creation_reraise(self, api, pkg, caplog):
        '''
        Test error handling during resource creation with on_error=reraise.
        '''
        with pytest.raises(ValueError):
            with pkg.sync_resource('x', on_error=OnError.reraise) as res:
                id = res['id']
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckanapi.NotFound):
            api.action.resource_show(id=id)

    def test_exception_during_resource_creation_keep(self, api, pkg, caplog):
        '''
        Test error handling during resource creation with on_error=keep.
        '''
        with pkg.sync_resource('x', on_error=OnError.keep) as res:
            id = res['id']
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckanapi.NotFound):
            api.action.resource_show(id=id)

    def test_exception_during_resource_creation_delete(self, api, pkg, caplog):
        '''
        Test error handling during resource creation with on_error=delete.
        '''
        with pkg.sync_resource('x', on_error=OnError.delete) as res:
            id = res['id']
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckanapi.NotFound):
            api.action.resource_show(id=id)

    def test_exception_during_resource_update_reraise(self, api, pkg, caplog):
        '''
        Test error handling during resource update with on_error=reraise.
        '''
        eid = 'x'
        with pkg.sync_resource(eid) as res:
            id = res['id']
            old_name = res['name']
        with pytest.raises(ValueError):
            with pkg.sync_resource(eid, on_error=OnError.reraise) as res:
                res['name'] = 'A new name'
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.resource_show(id=id)['name'] == old_name

    def test_exception_during_resource_update_keep(self, api, pkg, caplog):
        '''
        Test error handling during resource update with on_error=keep.
        '''
        eid = 'x'
        with pkg.sync_resource(eid) as res:
            id = res['id']
            old_name = res['name']
        with pkg.sync_resource(eid, on_error=OnError.keep) as res:
            res['name'] = 'A new name'
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.resource_show(id=id)['name'] == old_name

    def test_exception_during_resource_update_delete(self, api, pkg, caplog):
        '''
        Test error handling during resource update with on_error=delete.
        '''
        eid = 'x'
        with pkg.sync_resource(eid) as res:
            id = res['id']
        with pkg.sync_resource(eid, on_error=OnError.delete) as res:
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        with pytest.raises(ckanapi.NotFound):
            api.action.resource_show(id=id)


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

    def test_unmodified_view_update(self, res):
        '''
        Test not modifying an existing view.
        '''
        with res.sync_view('x') as view:
            view['view_type'] = 'text_view'
            view['title'] = 'Title'
        with mock.patch('ckanext.importer.View._upload') as upload:
            with res.sync_view('x'):
                pass
            upload.assert_not_called()

    def test_repr(self, res):
        assert repr(res) == '<Resource id={!r} eid={!r}>'.format(res['id'], res._eid)

    def test_delete_during_view_creation(self, api, res):
        '''
        Test deletion of a new view.
        '''
        with res.sync_view('a') as view:
            view['title'] = 'a'
            view['view_type'] = 'text_view'
        id_a = view['id']
        with res.sync_view('b') as view:
            view.delete()
        views = api.action.resource_view_list(id=res['id'])
        assert [view['id'] for view in views] == [id_a]

    def test_delete_during_view_update(self, api, res):
        '''
        Test deletion of an existing view.
        '''
        with res.sync_view('a') as view:
            view['title'] = 'a'
            view['view_type'] = 'text_view'
        id_a = view['id']
        with res.sync_view('b') as view:
            view['title'] = 'b'
            view['view_type'] = 'text_view'
        with res.sync_view('c') as view:
            view['title'] = 'c'
            view['view_type'] = 'text_view'
        id_c = view['id']
        with res.sync_view('b') as view:
            view.delete()
        views = api.action.resource_view_list(id=res['id'])
        assert [view['id'] for view in views] == [id_a, id_c]

    def test_delete_unsynced_views(self, api, pkg):
        '''
        Test deletion of unsynced views.
        '''
        res_eid = 'x'
        view_eids = [unicode(i) for i in range(13)]
        view_ids = {}
        with pkg.sync_resource(res_eid) as res:
            for view_eid in view_eids:
                with res.sync_view(view_eid) as view:
                    view['title'] = 'title'
                    view['view_type'] = 'text_view'
                view_ids[view_eid] = view['id']
        with pkg.sync_resource(res_eid) as res:
            with res.sync_view(view_eids[1]) as view:
                # Sync with changes
                view['title'] = 'A new title'
            with res.sync_view(view_eids[3]) as view:
                # Sync without any changes
                pass
            with pytest.raises(ValueError):
                with res.sync_view(view_eids[5], on_error=OnError.reraise) as view:
                    # Reraised error
                    raise ValueError('Oops')
            with res.sync_view(view_eids[7], on_error=OnError.keep) as view:
                # Swallowed error
                raise ValueError('Oops')
            with res.sync_view(view_eids[9], on_error=OnError.delete) as view:
                # Swallowed error with entity deletion
                raise ValueError('Oops')
            with res.sync_view(view_eids[11]) as view:
                # Delete
                view.delete()
            res.delete_unsynced_views()
        for view_eid in '0', '2', '4', '6', '8', '9', '10', '11', '12':
            with pytest.raises(ckanapi.NotFound):
                api.action.resource_view_show(id=view_ids[view_eid])
        for view_eid in '1', '3', '5', '7':
            api.action.resource_view_show(id=view_ids[view_eid])

    def test_exception_during_view_creation_reraise(self, api, res, caplog):
        '''
        Test error handling during view creation with on_error=reraise.
        '''
        with pytest.raises(ValueError):
            with res.sync_view('x', on_error=OnError.reraise):
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert not api.action.resource_view_list(id=res['id'])

    def test_exception_during_view_creation_keep(self, api, res, caplog):
        '''
        Test error handling during view creation with on_error=keep.
        '''
        with res.sync_view('x', on_error=OnError.keep):
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert not api.action.resource_view_list(id=res['id'])

    def test_exception_during_view_creation_delete(self, api, res, caplog):
        '''
        Test error handling during view creation with on_error=delete.
        '''
        with res.sync_view('x', on_error=OnError.delete):
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert not api.action.resource_view_list(id=res['id'])

    def test_exception_during_view_update_reraise(self, api, res, caplog):
        '''
        Test error handling during view update with on_error=reraise.
        '''
        eid = 'x'
        with res.sync_view(eid) as view:
            view['view_type'] = 'text_view'
            view['title'] = 'title'
        id = view['id']
        with pytest.raises(ValueError):
            with res.sync_view(eid, on_error=OnError.reraise):
                view['title'] = 'A new title'
                caplog.clear()
                raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.resource_view_show(id=id)['title'] == 'title'

    def test_exception_during_view_update_keep(self, api, res, caplog):
        '''
        Test error handling during view update with on_error=keep.
        '''
        eid = 'x'
        with res.sync_view(eid) as view:
            view['view_type'] = 'text_view'
            view['title'] = 'title'
        id = view['id']
        with res.sync_view(eid, on_error=OnError.keep):
            view['title'] = 'A new title'
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert api.action.resource_view_show(id=id)['title'] == 'title'

    def test_exception_during_view_update_delete(self, api, res, caplog):
        '''
        Test error handling during view update with on_error=delete.
        '''
        eid = 'x'
        with res.sync_view(eid) as view:
            view['view_type'] = 'text_view'
            view['title'] = 'title'
        with res.sync_view(eid, on_error=OnError.delete):
            caplog.clear()
            raise ValueError('Oops')
        assert caplog.records[0].levelno == logging.ERROR
        assert 'Oops' in caplog.records[0].message
        assert not api.action.resource_view_list(id=res['id'])


class TestView(object):

    def test_repr(self, res):
        eid = 'a'
        with res.sync_view(eid) as view:
            assert repr(view) == '<View eid={!r}>'.format(eid)
            view['view_type'] = 'text_view'
            view['title'] = 'title'
        assert repr(view) == '<View id={!r} eid={!r}>'.format(view['id'], eid)


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
