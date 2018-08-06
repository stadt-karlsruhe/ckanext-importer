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
import json
import logging
import re

import ckanapi


__version__ = '0.1.0'

_SOLR_ESCAPE_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[/\]^"~*?:])')

log = logging.getLogger(__name__)


def _solr_escape(s):
    '''
    Escape strings for Solr queries.
    '''
    return _SOLR_ESCAPE_RE.sub(r'\\\g<char>', s)


# Idea for starting a context manager on a method call:
#
# - Only a class can implement the CM protocol (using its __enter__ and
#   __exit__ methods), so accessing (not calling!) the target method must
#  return a suitable class.
#
# - That can be achieved using a descriptor class (that provides __get__ and
#   __set__ methods).
#
# - The nested CM should have knowledge of the outer CM. This is achieved
#   by wrapping the inner CM class in a function that creates an instance of
#   the inner class and then sets an appropriate attribute on it.
#
# - Finally we use a class for the wrapping, so that we can provide a
#   meaningful `repr`.
class _nested_cm_method(object):

    def __init__(self, cls):
        self._cls = cls

    def __get__(self, obj, type=None):

        class NestedCM(object):
            __name__ = self._cls.__name__
            __doc__ = self._cls.__doc__

            def __call__(_, *args, **kwargs):
                instance = self._cls.__new__(self._cls)
                instance._outer = obj
                instance.__init__(*args, **kwargs)
                return instance

            def __repr__(_):
                return '<nested context manager method {}.{} of {}>'.format(
                    obj.__class__.__name__,
                    self._cls.__name__,
                    obj,
                )

        return NestedCM()

    def __set__(self, obj, value):
        raise AttributeError('Read-only attribute')


_PACKAGE_NAME_PREFIX = 'ckanext_importer_'

class Importer(object):

    def __init__(self, id, api=None, default_owner_org=None):
        self.id = id
        self._api = api or ckanapi.LocalCKAN()
        self.default_owner_org = default_owner_org

    @_nested_cm_method
    class sync_package(object):
        '''
        Synchronize a package.
        '''
        def __init__(self, eid):
            self._eid = eid
            pkg_dicts = self._find_pkgs()
            if not pkg_dicts:
                self._pkg_dict = self._create_pkg()
                log.info('Created package {} for EID {}'.format(self._pkg_dict['id'], eid))
            elif len(pkg_dicts) > 1:
                raise ValueError('Multiple packages for EID {}'.format(eid))
            else:
                self._pkg_dict = pkg_dicts[0]
                log.debug('Using existing package {} for EID {}'.format(self._pkg_dict['id'], eid))

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
            self._package = Package(self._outer._api, self._pkg_dict)
            return self._package

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                log.error('Exception during synchronization of package {} (EID {}): {}'.format(
                    self._pkg_dict['id'], self._eid, exc_val))
                log.error('Not synchronizing that package.')
                return
            log.debug('Uploading updated version of package {} (EID {})'.format(self._pkg_dict['id'], self._eid))
            self._package._upload()


class DictWrapper(collections.MutableMapping):
    def __init__(self, d):
        self._dict = d

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __delitem__(self, key):
        del self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)


class Package(DictWrapper):
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
        self._api.action.package_update(**self._dict)

    @_nested_cm_method
    class sync_resource(object):
        def __init__(self, eid):
            self._eid = eid
            res_dicts = self._find_res()
            if not res_dicts:
                self._create_res()
                log.info('Created new resource {} for EID {}'.format(self._res_dict['id'], eid))
            elif len(res_dicts) > 1:
                raise ValueError('Multiple resources for EID {}'.format(eid))
            else:
                self._res_dict = res_dicts[0]
                log.debug('Using existing resource {} for EID {}'.format(self._res_dict['id'], eid))

        def _find_res(self):
            return [res_dict for res_dict in self._outer._dict['resources']
                    if res_dict['ckanext_importer_resource_eid'] == self._eid]

        def _create_res(self):
            self._res_dict = self._outer._api.action.resource_create(
                package_id=self._outer['id'],
                ckanext_importer_resource_eid=self._eid,
            )
            self._propagate_changes()

        def __enter__(self):
            return Resource(self._outer, self._res_dict)

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                log.error('Exception during synchronization of resource {} (EID {}): {}'.format(
                    self._res_dict['id'], self._eid, exc_val))
                log.error('Not synchronizing that resource.')
                return
            log.debug('Uploading updated version of resource {} (EID {})'.format(self._res_dict['id'], self._eid))
            # TODO: Perhaps we should only store the changes in the cached pkg dict
            #       and then upload only once (the whole package)? As long as we're
            #       not doing file uploads that should work.
            self._upload()

        def _upload(self):
            self._res_dict = self._outer._api.action.resource_update(**self._res_dict)
            self._propagate_changes()

        def _propagate_changes(self):
            '''
            Propagate changes in the cached resource dict to the cached package dict.
            '''
            for res_dict in self._outer._dict['resources']:
                if res_dict['id'] == self._res_dict['id']:
                    # Update existing cached resource
                    res_dict.clear()
                    res_dict.update(self._res_dict)
                    return
            # Append new cached resource
            self._outer._dict['resources'].append(self._res_dict)


class Resource(DictWrapper):
    '''
    Wrapper around a CKAN resource dict.

    Do not instantiate directly, use ``Package.sync_resource`` instead.
    '''
    def __init__(self, pkg, res_dict):
        super(Resource, self).__init__(res_dict)
        self._pkg = pkg

    @_nested_cm_method
    class sync_view(object):
        # Currently there is no way to attach extras to views (see
        # https://github.com/ckan/ckan/issues/2655), so we cannot
        # simply store the view's EID in the view itself. Instead,
        # we store that information in a separate resource extra.
        def __init__(self, eid):
            self._eid = eid

            views = json.loads(self._outer.get('ckanext_importer_views', '{}'))
            try:
                view_id = views[eid]
            except KeyError:
                # Ideally, we'd like to create a new view here (like we do for
                # packages and resources). However, CKAN's resource_view_create
                # requires us to fix the view's type, and resource_view_update
                # doesn't allow us to alter it afterwards. Hence we return an
                # empty dict here and do the creation when entering the context
                # manager.
                log.debug('Delaying view creation for EID {}'.format(eid))
                self._view_dict = {}
            else:
                self._view_dict = self._outer._pkg._api.action.resource_view_show(id=view_id)
                log.debug('Using existing view {} for EID {}'.format(view_id, eid))

        def __enter__(self):
            return self._view_dict

        def __exit__(self, exc_type, exc_val, exc_tb):
            view = self._view_dict
            if exc_type is not None:
                try:
                    id = view['id']
                except KeyError:
                    log.error('Exception during synchronization of new view (EID {}): {}'.format(
                              self._eid, exc_val))
                else:
                    log.error('Exception during synchronization of view {} (EID {}): {}'.format(
                              id, self._eid, exc_val))
                log.error('Not synchronizing that view.')
                return
            if 'id' in view:
                log.debug('Uploading updated version of view {} (EID {})'.format(view['id'], self._eid))
                self._view_dict = self._outer._pkg._api.action.resource_view_update(**view)
            else:
                # Create the view
                view['resource_id'] = self._outer['id']
                self._view_dict = self._outer._pkg._api.action.resource_view_create(**view)

                # Register it in the resource
                views = json.loads(self._outer.get('ckanext_importer_views', '{}'))
                views[self._eid] = self._view_dict['id']
                self._outer['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))

                log.info('Created view {} for EID {}'.format(self._view_dict['id'], self._eid))


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


if __name__ == '__main__':
    import io
    import json

    with io.open('apikey.json', 'r', encoding='utf-8') as f:
        apikey = json.load(f)['apikey']

    with ckanapi.RemoteCKAN('https://test-transparenz.karlsruhe.de', apikey=apikey) as api:
        imp = Importer(api, 'test-importer', 'stadt-karlsruhe')
        with imp.sync_package('peid1') as pkg:
            pkg_counter = int(pkg.extras.get('counter', 0))
            print('package counter = {!r}'.format(pkg_counter))
            pkg.extras['counter'] = pkg_counter + 1

            with io.open('test.csv', 'r', encoding='utf-8') as upload:

                with pkg.sync_resource('reid1') as res:
                    res_counter = int(res.get('counter', 0))
                    print('resource counter = {!r}'.format(res_counter))
                    res['counter'] = res_counter + 1

                    # File upload
                    print('resource url = {!r}'.format(res['url']))
                    res['upload'] = upload

                    with res.sync_view('veid2') as view:
                        view['view_type'] = 'text_view'

                        counter = int(view.get('title', 0))
                        print('view counter = {!r}'.format(counter))
                        view['title'] = counter + 1

