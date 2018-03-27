#!/usr/bin/env python

import logging
import re


log = logging.getLogger(__name__)


_SOLR_ESCAPE_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[/\]^"~*?:])')


def solr_escape(s):
    '''
    Escape strings for Solr queries.
    '''
    return _SOLR_ESCAPE_RE.sub(r'\\\g<char>', s)


def _get_extra(pkg, key):
    '''
    Get the value of a package extra.
    '''
    for extra in pkg['extras']:
        if extra['key'] == key:
            return extra['value']
    raise KeyError(key)


class Importer(object):

    def __init__(self, id, api):
        '''
        ``id``: ID-string of the importer. Used to identify which
        existing datasets belong to the importer.
        '''
        self.id = id
        self.api = api

    def call_action(self, method_name, **kwargs):
        return self.api.call_action(method_name, kwargs)

    def sync(self, master):
        '''
        ``master`` is a list of dataset descriptions.
        '''
        master_pkgs = {pkg['_id']: pkg for pkg in self._check_master(master)}

        # Find all existing datasets that belong to the importer
        existing_pkgs = self._find_pkgs_by_extra('ckanext_importer_importer_id', solr_escape(self.id))
        log.debug('Found {} existing package(s).'.format(len(existing_pkgs)))

        # For each existing dataset: if it has a master, update it, otherwise remove it
        for existing_pkg in existing_pkgs:
            dataset_id = _get_extra(existing_pkg, 'ckanext_importer_dataset_id')
            try:
                master_pkg = master_pkgs.pop(dataset_id)
            except KeyError:
                log.info('No master for existing dataset {} (CKAN ID {}), removing it.'.format(dataset_id, existing_pkg['id']))
                # TODO: Remove the existing dataset
                continue
            self._sync_pkg(existing_pkg, master_pkg)

        # For the remaining master datasets: create real datasets
        for master_pkg in master_pkgs.values():
            self._create_pkg(master_pkg)

    def _check_master(self, master):
        with_id = [pkg for pkg in master if '_id' in pkg]
        if len(with_id) != len(master):
            log.warning('Ignoring {} dataset(s) without "_id" attribute.'.format(len(master) - len(with_id)))
        with_org = [pkg for pkg in master if '_organization' in pkg]
        if len(with_org) != len(with_id):
            log.warning('Ignoring {} dataset(s) without "_organization" attribute.'.format(len(with_id) - len(with_org)))
        unique_ids = set(pkg['_id'] for pkg in with_org)
        if len(unique_ids) != len(with_org):
            log.warning('Found {} dataset master(s) with duplicate "_id" values.'.format(len(with_org) - len(unique_ids)))
        return with_org

    def _sync_pkg(self, existing, master):
        '''
        Synch a single dataset.

        ``existing`` is the package dict of an existing dataset.

        ``master`` is the description for that dataset.
        '''
        log.info('Synchronizing dataset {} (CKAN ID {})'.format(master['_id'], existing['id']))
        # TODO


    def _create_pkg(self, master):
        '''
        Create a single dataset.

        ``master`` is the description for that dataset.

        CKAN errors are logged and swallowed.
        '''
        id = master['_id']
        log.debug('Creating new dataset {}'.format(id))

        new_pkg = {key: value for key, value in master.items() if key[0] != '_'}

        special_keys = set(key for key in master if key[0] == '_')
        unknown_keys = special_keys.difference({'_id', '_organization'})
        if unknown_keys:
            log.warning('Ignoring unknown special key(s) {} in dataset {}'.format(list(unknown_keys), id))

        new_pkg.setdefault('extras', []).extend([
            {'key': 'ckanext_importer_importer_id', 'value': self.id},
            {'key': 'ckanext_importer_dataset_id', 'value': id},
        ])

        new_pkg['owner_org'] = master['_organization']

        try:
            new_pkg = self.call_action('package_create', **new_pkg)
        except Exception as e:
            log.error('Error while creating new dataset {}: {}'.format(id, e))
            return

        log.info('New dataset {} has CKAN ID {}'.format(id, new_pkg['id']))

    def _find_pkgs_by_extra(self, key, value='*'):
        '''
        Find CKAN packages with a certain "extra" field.

        ``key`` is the name of the extra field.

        ``value`` is the (string) value to look for. Note that you might
        need to escape Solr special characters (if you don't want them
        to be interpreted specially). See ``solr_escape``.

        Returns a list of package dicts.
        '''
        fq = 'extras_{key}:"{value}"'.format(key=key, value=value)
        # FIXME: Support for paging
        return self.call_action('package_search', fq=fq, rows='1000')['results']


if __name__ == '__main__':
    import io
    import json

    from ckanapi import RemoteCKAN

    log.addHandler(logging.StreamHandler())
    log.setLevel(logging.DEBUG)

    master = [
        {
            'title': 'No _id to test the warning',
        },
        {
            'title': 'No _organization to test the warning',
            '_id': 'invalid',
        },
        {
            '_id': 'test-01',
            'name': 'a-first-test',
            '_organization': 'stadt-karlsruhe',
        },
        {
            '_id': 'test-02',
            'name': 'a-test-with-extras',
            'extras': [
                {'key': 'something', 'value': 'special'},
            ],
            '_organization': 'stadt-karlsruhe',
            '_unknown1': 'An unknown special key',
            '_unknown2': 'Another unknown special key',
        },
    ]

    with io.open('apikey.json', 'r', encoding='utf-8') as f:
        apikey = json.load(f)['apikey']

    with RemoteCKAN('https://test-transparenz.karlsruhe.de', apikey=apikey) as api:
        importer = Importer('test-importer-01', api)
        importer.sync(master)

