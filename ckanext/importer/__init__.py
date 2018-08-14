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

import collections
from enum import Enum
import json
import logging

import ckanapi

from .utils import DictWrapper, context_manager_method, replace_dict, solr_escape


__version__ = '0.1.0'


log = logging.getLogger(__name__)


class SyncMode(Enum):
    '''
    How to sync a package, resource, or view.
    '''
    sync = 1       # Sync normally
    dont_sync = 2  # Don't sync (keep the existing version)
    delete = 3     # Delete the existing version


class Entity(DictWrapper):
    '''
    Base class for package, resource, and view wrappers.

    Not to be instantiated directly.
    '''
    def __init__(self, eid, d):
        '''
        Constructor.

        ``d`` is the dict wrapped by this entity.
        '''
        super(Entity, self).__init__(d)
        self._eid = eid
        self.sync_mode = SyncMode.sync

    def delete(self):
        '''
        Mark this entity for deletion.
        '''
        self.sync_mode = SyncMode.delete

    def dont_sync(self):
        '''
        Mark this entity for no synchronization.
        '''
        self.sync_mode = SyncMode.dont_sync

    def _delete(self):
        '''
        Actually delete this entity.

        Must be implemented by subclasses.
        '''
        raise NotImplementedError()

    def _upload(self):
        '''
        Upload this entity.

        Must be implemented by subclasses.
        '''
        raise NotImplementedError()

    def __repr__(self):
        try:
            id_part = 'id={!r} '.format(self['id'])
        except KeyError:
            id_part = ''
        return '<{} {}eid={!r}>'.format(self.__class__.__name__,
                                        id_part, self._eid)


class EntitySyncManager(object):
    '''
    Context manager for synchronizing an ``Entity``.

    Do not instantiate directly.
    '''
    def __init__(self, eid):
        self._eid = unicode(eid)
        self._entity = self._prepare_entity()
        assert self._entity is not None

    def _prepare_entity(self):
        '''
        Find, create, or otherwise prepare the entity.

        Subclasses must implement this method to return an entity based
        on ``_eid``.
        '''
        raise NotImplementedError()

    def __enter__(self):
        return self._entity

    def __exit__(self, exc_type, exc_val, exc_tb):
        entity = self._entity
        if exc_type is not None:
            log.error('An error occured during the synchronization of {}: {}'.format(entity, exc_val),
                      exc_info=(exc_type, exc_val, exc_tb))
            log.error('Changes to {} will not be uploaded'.format(entity))
            return
        if entity.sync_mode == SyncMode.sync:
            log.debug('Uploading {}'.format(entity))
            entity._upload()
        elif entity.sync_mode == SyncMode.dont_sync:
            log.debug('{} is marked as "dont sync"'.format(entity))
        elif entity.sync_mode == SyncMode.delete:
            log.debug('{} is marked as "delete", removing it'.format(entity))
            entity._delete()
        else:
            raise ValueError('Unknown sync mode {} for {}'.format(entity.sync_mode, entity))


_PACKAGE_NAME_PREFIX = 'ckanext_importer_'

class Importer(object):

    def __init__(self, id, api=None, default_owner_org=None):
        self.id = unicode(id)
        self._api = api or ckanapi.LocalCKAN()
        self.default_owner_org = default_owner_org

    @context_manager_method
    class sync_package(EntitySyncManager):
        def _prepare_entity(self):
            pkg_dicts = self._find_pkgs()
            if not pkg_dicts:
                pkg_dict = self._create_pkg()
                pkg = Package(self._eid, pkg_dict, self._outer._api)
                log.debug('Created {}'.format(pkg))
                return pkg
            elif len(pkg_dicts) > 1:
                raise ValueError('Multiple packages for EID {}'.format(self._eid))
            else:
                pkg = Package(self._eid, pkg_dicts[0], self._outer._api)
                log.debug('Using {}'.format(pkg))
                return pkg

        def _create_pkg(self):
            '''
            Create a new CKAN package.

            Takes care of finding an unused name and of setting the
            required ckanext.importer metadata.
            '''
            i = 0
            while True:
                name = '{}{}'.format(_PACKAGE_NAME_PREFIX, i)
                try:
                    return self._outer._api.action.package_create(
                        name=name,
                        owner_org=self._outer.default_owner_org,
                        extras=[
                            {'key': 'ckanext_importer_importer_id',
                             'value': self._outer.id},
                            {'key': 'ckanext_importer_package_eid',
                             'value': self._eid},
                        ],
                    )
                except ckanapi.ValidationError as e:
                    if 'name' in e.error_dict:
                        # Duplicate name
                        i += 1
                        continue
                    raise

        def _find_pkgs(self):
            '''
            Find existing packages with the given EID.
            '''
            extras = {
                'ckanext_importer_importer_id': solr_escape(self._outer.id),
                'ckanext_importer_package_eid': solr_escape(self._eid),
            }
            fq = ' AND '.join('extras_{}:"{}"'.format(*item)
                              for item in extras.items())
            # FIXME: Support for paging
            result = self._outer._api.action.package_search(fq=fq, rows=1000)

            # CKAN's search is based on Solr, which by default doesn't support
            # searching for exact matches. Hence searching for importer ID "x"
            # can also return packages with importer ID "x-y". Hence we filter
            # the results again.
            pkgs = []
            for pkg in result['results']:
                extras = ExtrasDictView(pkg['extras'])
                if (extras['ckanext_importer_importer_id'] == self._outer.id and
                    extras['ckanext_importer_package_eid'] == self._eid):
                    pkgs.append(pkg)

            return pkgs

    def __repr__(self):
        return '<{} id={!r}>'.format(self.__class__.__name__, self.id)


class Package(Entity):
    '''
    Wrapper around a CKAN package dict.

    Not to be instantiated directly. Use ``Importer.sync_package``
    instead.
    '''
    def __init__(self, eid, pkg_dict, api):
        super(Package, self).__init__(eid, pkg_dict)
        self._api = api
        self.extras = ExtrasDictView(pkg_dict['extras'])

    def _upload(self):
        '''
        Upload package dict to CKAN.
        '''
        replace_dict(self,
                     self._api.action.package_update(**self))

    def _delete(self):
        '''
        Purge this package.
        '''
        self._api.action.dataset_purge(id=self['id'])

    @context_manager_method
    class sync_resource(EntitySyncManager):
        def _prepare_entity(self):
            res_dicts = [r for r in self._outer['resources']
                         if r['ckanext_importer_resource_eid'] == self._eid]
            if not res_dicts:
                res_dict = self._outer._api.action.resource_create(
                    package_id=self._outer['id'],
                    ckanext_importer_resource_eid=self._eid,
                )
                self._outer['resources'].append(res_dict)
                res = Resource(self._eid, res_dict, self._outer)
                log.info('Created {}'.format(res))
                return res
            elif len(res_dicts) > 1:
                raise ValueError('Multiple resources for EID {} in {}'.format(self._eid, self._outer))
            else:
                res = Resource(self._eid, res_dicts[0], self._outer)
                log.debug('Using {}'.format(res))
                return res


class Resource(Entity):
    '''
    Wrapper around a CKAN resource dict.

    Do not instantiate directly, use ``Package.sync_resource`` instead.
    '''
    def __init__(self, eid, res_dict, pkg):
        super(Resource, self).__init__(eid, res_dict)
        self._pkg = pkg

    def _delete(self):
        id = self['id']
        self._pkg._api.action.resource_delete(id=id)
        self._pkg['resources'][:] = [r for r in self._pkg['resources']
                                     if r['id'] != id]

    def _upload(self):
        '''
        Upload the modified resource dict and propagate the changes.
        '''
        replace_dict(self, self._pkg._api.action.resource_update(**self))

    def _get_views_map(self):
        '''
        Get the map of views for this resource.
        '''
        return json.loads(self.get('ckanext_importer_views', '{}'))

    def _set_views_map(self, views):
        '''
        Set the map of views for this resource.
        '''
        self['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))

    @context_manager_method
    class sync_view(EntitySyncManager):
        def _prepare_entity(self):
            views = self._outer._get_views_map()
            try:
                id = views[self._eid]
            except KeyError:
               # Ideally, we'd like to create a new view here (like we do for
                # packages and resources). However, CKAN's resource_view_create
                # requires us to fix the view's type, and resource_view_update
                # doesn't allow us to alter it afterwards. Hence we return an
                # empty dict here and do the creation when entering the context
                # manager.
                log.debug('Delaying view creation for EID {}'.format(self._eid))
                return View(self._eid, {}, self._outer)
            else:
                view_dict = self._outer._pkg._api.action.resource_view_show(id=id)
                view = View(self._eid, view_dict, self._outer)
                log.debug('Using {}'.format(view))
                return view


class View(Entity):
    '''
    Wrapper around a CKAN view.

    Do not instantiate directly. Use ``Resource.sync_view`` instead.
    '''

    # FIXME: When we update the resource's ckanext_importer_views field
    #         then the upstream res dict is only updated once the
    #         sync_resource CM exits. If this doesn't happen (due
    #         to an exception or a call to dont_sync) then we end
    #         up with an already created/deleted view that isn't
    #         properly tracked by ckanext.importer. At the very least we
    #         should automatically discover such issues, better would be
    #         to prevent them in the first place.

    def __init__(self, eid, view_dict, res):
        super(View, self).__init__(eid, view_dict)
        self._res = res

    def _upload(self):
        try:
            id = self['id']
        except KeyError:
            self._create()
        else:
            replace_dict(self,
                         self._res._pkg._api.action.resource_view_update(**self))

    def _create(self):
        '''
        Create a view.
        '''
        self['resource_id'] = self._res['id']
        replace_dict(self,
                     self._res._pkg._api.action.resource_view_create(**self))
        # Register the view in the resource
        views = self._res._get_views_map()
        views[self._eid] = self['id']
        self._res._set_views_map(views)

    def _delete(self):
        try:
            id = self['id']
        except KeyError:
            # View has not been created yet
            return
        self._res._pkg._api.action.resource_view_delete(id=id)
        # Unregister the view in the resource
        views = self._res._get_views_map()
        del views[self._eid]
        self._res._set_views_map(views)


class ExtrasDictView(collections.MutableMapping):
    '''
    Wrapper around a CKAN package's "extras".
    '''
    def __init__(self, extras):
        '''
        Constructor.

        ``extras`` is a list of package extras. That list is managed
        in-place by the created ``ExtrasDictView`` instance.
        '''
        self._extras = extras

    def __getitem__(self, key):
        for extra in self._extras:
            if extra['key'] == key:
                return extra['value']
        raise KeyError(key)

    def __setitem__(self, key, value):
        '''
        Set the value of a package extra.

        If there is an existing extra with the given key its value is
        replaced. Otherwise, a new extra is appended at the end of the
        extras list.
        '''
        for extra in self._extras:
            if extra['key'] == key:
                extra['value'] = value
                return
        self._extras.append({'key': key, 'value': value})

    def __delitem__(self, key):
        for i, extra in enumerate(self._extras):
            if extra['key'] == key:
                self._extras.pop(i)
                return
        raise KeyError(key)

    def __len__(self):
        return len(self._extras)

    def __iter__(self):
        return (extra['key'] for extra in self._extras)
