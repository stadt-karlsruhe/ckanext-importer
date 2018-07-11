#!/usr/bin/env python2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json

import lib


_PACKAGE_NAME_PREFIX = 'ckanext_importer_'

class Importer(object):

    def __init__(self, api, id, default_owner_org):
        self._api = api
        self.id = id
        self.default_owner_org = default_owner_org

    @lib.nested_cm_method
    class sync_package(object):
        '''
        Synchronize a package.
        '''
        def __init__(self, eid):
            #print('sync_package.__init__')
            self._eid = eid
            pkg_dicts = self._find_pkgs()
            if not pkg_dicts:
                self._pkg_dict = self._create_pkg()
                print('Created package {} for EID {}'.format(self._pkg_dict['id'], eid))
            elif len(pkg_dicts) > 1:
                raise ValueError('Multiple packages for EID {}'.format(eid))
            else:
                self._pkg_dict = pkg_dicts[0]
                print('Using existing package {} for EID {}'.format(self._pkg_dict['id'], eid))

        def _find_pkgs(self):
            return lib.find_pkgs_by_extras(
                self._outer._api,
                {
                    'ckanext_importer_importer_id': lib.solr_escape(self._outer.id),
                    'ckanext_importer_package_eid': lib.solr_escape(self._eid),
                },
            )

        def _create_pkg(self):
            return lib.create_package(
                self._outer._api,
                _PACKAGE_NAME_PREFIX,
                owner_org=self._outer.default_owner_org,
                extras=[
                    {'key': 'ckanext_importer_importer_id',
                     'value': self._outer.id},
                    {'key': 'ckanext_importer_package_eid',
                     'value': self._eid},
                ],
            )

        def __enter__(self):
            #print('sync_package.__enter__')
            self._package = Package(self._outer._api, self._pkg_dict)
            return self._package

        def __exit__(self, exc_type, exc_val, exc_tb):
            #print('sync_package.__exit__')
            if exc_type is not None:
                print('Exception during synchronization of package {} (EID {}): {}'.format(
                    self._pkg_dict['id'], self._eid, exc_val))
                print('Not synchronizing that package.')
                return
            print('Uploading updated version of package {} (EID {})'.format(self._pkg_dict['id'], self._eid))
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
        self._api.action.package_update(**self._dict)

    @lib.nested_cm_method
    class sync_resource(object):
        def __init__(self, eid):
            self._eid = eid
            res_dicts = self._find_res()
            if not res_dicts:
                self._create_res()
                print('Created new resource {} for EID {}'.format(self._res_dict['id'], eid))
            elif len(res_dicts) > 1:
                raise ValueError('Multiple resources for EID {}'.format(eid))
            else:
                self._res_dict = res_dicts[0]
                print('Using existing resource {} for EID {}'.format(self._res_dict['id'], eid))

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
            #print('sync_resource.__enter__')
            return Resource(self._outer, self._res_dict)

        def __exit__(self, exc_type, exc_val, exc_tb):
            #print('sync_resource.__exit__')
            if exc_type is not None:
                print('Exception during synchronization of resource {} (EID {}): {}'.format(
                    self._res_dict['id'], self._eid, exc_val))
                print('Not synchronizing that resource.')
                return
            print('Uploading updated version of resource {} (EID {})'.format(self._res_dict['id'], self._eid))
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

    @lib.nested_cm_method
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
                self._view_dict = self._outer._pkg._api.action.resource_view_create(
                    resource_id=self._outer['id'],
                    view_type='text_view',
                    title='ckanext-importer default title',
                )
                views[eid] = self._view_dict['id']
                self._outer['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))
                print('Created view {} for EID {}'.format(self._view_dict['id'], eid))
            else:
                self._view_dict = self._outer._pkg._api.action.resource_view_show(id=view_id)
                print('Using existing view {} for EID {}'.format(view_id, eid))

        def __enter__(self):
            return self._view_dict

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                print('Exception during synchronization of view {} (EID {}): {}'.format(
                    self._view_dict['id'], self._eid, exc_val))
                print('Not synchronizing that view.')
                return
            print('Uploading updated version of view {} (EID {})'.format(self._view_dict['id'], self._eid))
            self._view_dict = self._outer._pkg._api.action.resource_view_update(**self._view_dict)


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

    from ckanapi import RemoteCKAN


    with io.open('apikey.json', 'r', encoding='utf-8') as f:
        apikey = json.load(f)['apikey']

    with RemoteCKAN('https://test-transparenz.karlsruhe.de', apikey=apikey) as api:
        imp = Importer(api, 'test-importer', 'stadt-karlsruhe')
        with imp.sync_package('peid1') as pkg:
            pkg_counter = int(pkg.extras.get('counter', 0))
            print('package counter = {!r}'.format(pkg_counter))
            pkg.extras['counter'] = pkg_counter + 1

            with pkg.sync_resource('reid1') as res:
                res_counter = int(res.get('counter', 0))
                print('resource counter = {!r}'.format(res_counter))
                res['counter'] = res_counter + 1

                with res.sync_view('veid1') as view:
                    try:
                        counter = int(view['title'])
                    except ValueError:
                        counter = 0
                    print('view counter = {!r}'.format(counter))
                    view['title'] = counter + 1

