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
import re

import ckanapi

from .utils import DictWrapper, context_manager_method, replace_dict


__version__ = '0.1.0'

_SOLR_ESCAPE_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[/\]^"~*?:])')

log = logging.getLogger(__name__)


def _solr_escape(s):
    '''
    Escape strings for Solr queries.
    '''
    return _SOLR_ESCAPE_RE.sub(r'\\\g<char>', s)


_PACKAGE_NAME_PREFIX = 'ckanext_importer_'

class Importer(object):

    def __init__(self, id, api=None, default_owner_org=None):
        self.id = unicode(id)
        self._api = api or ckanapi.LocalCKAN()
        self.default_owner_org = default_owner_org

    @context_manager_method
    class sync_package(object):
        '''
        Synchronize a package.
        '''
        def __init__(self, eid):
            self._eid = unicode(eid)
            pkg_dicts = self._find_pkgs()
            if not pkg_dicts:
                self._pkg_dict = self._create_pkg()
                log.info('Created package {} for EID {}'.format(self._pkg_dict['id'], self._eid))
            elif len(pkg_dicts) > 1:
                raise ValueError('Multiple packages for EID {}'.format(self._eid))
            else:
                self._pkg_dict = pkg_dicts[0]
                log.debug('Using existing package {} for EID {}'.format(self._pkg_dict['id'], self._eid))
            self._package = Package(self._outer._api, self._pkg_dict)

        def _find_pkgs(self):
            extras = {
                'ckanext_importer_importer_id': _solr_escape(self._outer.id),
                'ckanext_importer_package_eid': _solr_escape(self._eid),
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


        def _create_pkg(self):
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

        def __enter__(self):
            return self._package

        def __exit__(self, exc_type, exc_val, exc_tb):
            pkg = self._package
            if exc_type is not None:
                log.error('Exception during synchronization of package {} (EID {}): {}'.format(
                    pkg['id'], self._eid, exc_val))
                log.error('Not synchronizing that package.')
                return
            if pkg.sync_mode == SyncMode.sync:
                log.debug('Uploading updated version of package {} (EID {})'.format(pkg['id'], self._eid))
                pkg._upload()
            elif pkg.sync_mode == SyncMode.dont_sync:
                log.debug('Package {} (EID {}) is marked as "dont sync"'.format(pkg['id'], self._eid))
            elif pkg.sync_mode == SyncMode.delete:
                log.debug('Package {} (EID {}) is marked as "delete", removing it'.format(pkg['id'], self._eid))
                pkg._purge()
            else:
                raise ValueError('Unknown sync mode {} for package {} (EID {})'.format(
                                 pkg.sync_mode, pkg['id'], self._eid))

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.id)


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
    def __init__(self, d):
        '''
        Constructor.

        ``d`` is the dict wrapped by this entity.
        '''
        super(Entity, self).__init__(d)
        self.sync_mode = SyncMode.sync

    def delete(self):
        '''
        Delete this entity.
        '''
        self.sync_mode = SyncMode.delete

    def dont_sync(self):
        '''
        Do not sync this entity.
        '''
        self.sync_mode = SyncMode.dont_sync


class Package(Entity):
    '''
    Wrapper around a CKAN package dict.

    Not to be instantiated directly. Use ``Importer.sync_package``
    instead.
    '''
    def __init__(self, api, pkg_dict):
        super(Package, self).__init__(pkg_dict)
        self._api = api
        self.extras = ExtrasDictView(pkg_dict['extras'])

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self['id'])

    def _upload(self):
        '''
        Upload package dict to CKAN.
        '''
        log.debug('Package._upload: self._dict = {}'.format(self._dict))
        replace_dict(self._dict,
                     self._api.action.package_update(**self._dict))

    def _purge(self):
        '''
        Purge this package.
        '''
        self._api.action.dataset_purge(id=self._dict['id'])

    @context_manager_method
    class sync_resource(object):
        def __init__(self, eid):
            self._eid = unicode(eid)
            res_dicts = self._find_res()
            if not res_dicts:
                self._create_res()
                log.info('Created new resource {} for EID {}'.format(self._res_dict['id'], self._eid))
            elif len(res_dicts) > 1:
                raise ValueError('Multiple resources for EID {}'.format(self._eid))
            else:
                self._res_dict = res_dicts[0]
                log.debug('Using existing resource {} for EID {}'.format(self._res_dict['id'], self._eid))
            self._resource = Resource(self._outer, self._res_dict)

        def _find_res(self):
            return [res_dict for res_dict in self._outer._dict['resources']
                    if res_dict['ckanext_importer_resource_eid'] == self._eid]

        def _create_res(self):
            self._res_dict = self._outer._api.action.resource_create(
                package_id=self._outer['id'],
                ckanext_importer_resource_eid=self._eid,
            )
            self._outer._dict['resources'].append(self._res_dict)

        def __enter__(self):
            return self._resource

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                log.error('Exception during synchronization of resource {} (EID {}): {}'.format(
                    self._res_dict['id'], self._eid, exc_val))
                log.error('Not synchronizing that resource.')
                return
            res = self._resource
            if res.sync_mode == SyncMode.sync:
                log.debug('Uploading updated version of resource {} (EID {})'.format(res['id'], self._eid))
                self._upload()
            elif res.sync_mode == SyncMode.dont_sync:
                log.debug('Resource {} (EID {}) is marked as "dont sync"'.format(res['id'], self._eid))
            elif res.sync_mode == SyncMode.delete:
                log.debug('Resource {} (EID {}) is marked as "delete", removing it'.format(res['id'], self._eid))
                self._delete()
            else:
                raise ValueError('Unknown sync mode {} for resource {} (EID {})'.format(
                                 res.sync_mode, res['id'], self._eid))

        def _upload(self):
            '''
            Upload the modified resource dict and propagate the changes.
            '''
            replace_dict(self._res_dict,
                         self._outer._api.action.resource_update(**self._res_dict))

        def _delete(self):
            '''
            Delete the resource and propagate the changes.
            '''
            id = self._res_dict['id']
            self._outer._api.action.resource_delete(id=id)
            self._outer._dict['resources'][:] = [r for r in self._outer._dict['resources']
                                                 if r['id'] != id]


class Resource(Entity):
    '''
    Wrapper around a CKAN resource dict.

    Do not instantiate directly, use ``Package.sync_resource`` instead.
    '''
    def __init__(self, pkg, res_dict):
        super(Resource, self).__init__(res_dict)
        self._pkg = pkg

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self['id'])

    @context_manager_method
    class sync_view(object):
        # Currently there is no way to attach extras to views (see
        # https://github.com/ckan/ckan/issues/2655), so we cannot
        # simply store the view's EID in the view itself. Instead,
        # we store that information in a separate resource extra.
        def __init__(self, eid):
            self._eid = unicode(eid)

            views = json.loads(self._outer.get('ckanext_importer_views', '{}'))
            try:
                view_id = views[self._eid]
            except KeyError:
                # Ideally, we'd like to create a new view here (like we do for
                # packages and resources). However, CKAN's resource_view_create
                # requires us to fix the view's type, and resource_view_update
                # doesn't allow us to alter it afterwards. Hence we return an
                # empty dict here and do the creation when entering the context
                # manager.
                log.debug('Delaying view creation for EID {}'.format(self._eid))
                self._view_dict = {}
            else:
                self._view_dict = self._outer._pkg._api.action.resource_view_show(id=view_id)
                log.debug('Using existing view {} for EID {}'.format(view_id, self._eid))
            self._view = View(self._outer, self._view_dict)

        def __enter__(self):
            return self._view

        def __exit__(self, exc_type, exc_val, exc_tb):
            view = self._view
            id = view.get('id', None)
            if exc_type is not None:
                if id:
                    log.error('Exception during synchronization of new view (EID {}): {}'.format(
                              self._eid, exc_val))
                else:
                    log.error('Exception during synchronization of view {} (EID {}): {}'.format(
                              id, self._eid, exc_val))
                log.error('Not synchronizing that view.')
                return
            if view.sync_mode == SyncMode.sync:
                if id:
                    log.debug('Uploading updated version of view {} (EID {})'.format(view['id'], self._eid))
                    self._upload()
                else:
                    log.debug('Creating view for EID {}'.format(self._eid))
                    self._create()
            elif view.sync_mode == SyncMode.dont_sync:
                if id:
                    log.debug('View {} (EID {}) is marked as "dont sync"'.format(view['id'], self._eid))
                else:
                    log.debug('New view (EID {}) is marked as "dont sync"'.format(self._eid))
            elif view.sync_mode == SyncMode.delete:
                if id:
                    log.debug('View {} (EID {}) is marked as "delete", removing it'.format(view['id'], self._eid))
                    self._delete()
                else:
                    log.debug('New view (EID {}) is marked as "delete", not creating it'.format(self._eid))
            else:
                raise ValueError('Unknown sync mode {} for view {} (EID {})'.format(
                                 view.sync_mode, view['id'], self._eid))

        def _upload(self):
            '''
            Upload the modified view dict.
            '''
            replace_dict(self._view_dict,
                         self._outer._pkg._api.action.resource_view_update(**self._view_dict))

        def _create(self):
            '''
            Create a view.
            '''
            self._view_dict['resource_id'] = self._outer['id']
            replace_dict(self._view_dict,
                         self._outer._pkg._api.action.resource_view_create(**self._view_dict))

            # Register the view in the resource
            views = json.loads(self._outer.get('ckanext_importer_views', '{}'))
            views[self._eid] = self._view_dict['id']
            self._outer['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))
            # FIXME: This adds the view to the local res dict, but the
            #         upstream res dict is only updated once the
            #         sync_resource CM exits. If this doesn't happen (due
            #         to an exception or a call to dont_sync) then we end
            #         up with an already created view on the resource that
            #         isn't properly tracked by ckanext.importer. At the
            #         very least we should automatically discover such
            #         stray views, better would be to prevent it in the
            #         first place.

            log.info('Created view {} for EID {}'.format(self._view_dict['id'], self._eid))

        def _delete(self):
            '''
            Delete the view.
            '''
            self._outer._pkg._api.action.resource_view_delete(id=self._view_dict['id'])

            # Unregister the view in the resource
            views = json.loads(self._outer.get('ckanext_importer_views', '{}'))
            views.pop(self._eid)
            self._outer['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))


class View(Entity):
    '''
    Wrapper around a CKAN view.

    Do not instantiate directly. Use ``Resource.sync_view`` instead.
    '''
    def __init__(self, res, view_dict):
        super(View, self).__init__(view_dict)
        self._res = res

    def __repr__(self):
        try:
            id = self['id']
        except KeyError:
            return '<{} [to-be-created]>'.format(self.__class__.__name__)
        return '<{} {}>'.format(self.__class__.__name__, id)


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
