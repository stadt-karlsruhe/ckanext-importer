#!/usr/bin/env python2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections

import lib


_PACKAGE_NAME_PREFIX = 'ckanext-importer-'

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
            print('SyncPackage.__init__')

            pkg_dicts = lib.find_pkgs_by_extras(
                self._outer._api,
                {
                    'ckanext-importer-importer-id': lib.solr_escape(self._outer.id),
                    'ckanext-importer-package-eid': lib.solr_escape(eid),
                },
            )
            if not pkg_dicts:
                self._pkg_dict = lib.create_package(
                    self._outer._api,
                    _PACKAGE_NAME_PREFIX,
                    owner_org=self._outer.default_owner_org,
                    extras=[
                        {'key': 'ckanext-importer-importer-id',
                         'value': self._outer.id},
                        {'key': 'ckanext-importer-package-eid',
                         'value': eid},
                    ],
                )
                print('Created package {} for EID {}'.format(self._pkg_dict['id'], eid))
            elif len(pkg_dicts) > 1:
                raise ValueError('Multiple packages for EID {}'.format(eid))
            else:
                self._pkg_dict = pkg_dicts[0]
                print('Using existing package {} for EID {}'.format(self._pkg_dict['id'], eid))


        def __enter__(self):
            print('SyncPackage.__enter__')
            return Package(self._pkg_dict)

        def __exit__(self, exc_type, exc_val, exc_tb):
            print('SyncPackage.__exit__')


class Package(collections.MutableMapping):
    '''
    Wrapper around a CKAN package dict.
    '''
    def __init__(self, pkg_dict):
        self._pkg_dict = pkg_dict
        self.extras = ExtrasDictView(pkg_dict['extras'])

    def __getitem__(self, key):
        return self._pkg_dict[key]

    def __setitem__(self, key, value):
        self._pkg_dict[key] = value

    def __delitem__(self, key):
        del self._pkg_dict[key]

    def __iter__(self):
        return iter(self._pkg_dict)

    def __len__(self):
        return len(self._pkg_dict)

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self['id'])


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
        with imp.sync_package('eid1') as pkg:
            print('Package extras:')
            for extra in pkg.extras:
                print('  {}'.format(extra))

