#!/usr/bin/env python2

import copy
from itertools import izip_longest
import logging
import re

import jsondiff


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


def _set_extra(pkg, key, value):
    '''
    Set the value of a package extra.

    If there is an existing extra with the given key its value is
    replaced. Otherwise, a new extra is appended at the end of the
    extras list.
    '''
    extras = pkg.setdefault('extras', [])
    for extra in extras:
        if extra['key'] == key:
            extra['value'] = value
            return
    extras.append({'key': key, 'value': value})


class Importer(object):

    def __init__(self, id, api):
        '''
        ``id``: ID-string of the importer. Used to identify which
        existing datasets belong to the importer.
        '''
        self.id = id
        self.api = api

    def _upload_resource_file(self, filename, res):
        '''
        Upload a file as a resource's data.

        ``filename`` is the name of the file.

        ``res`` is the resource dict.

        The resource must already exist.
        '''
        res = copy.deepcopy(res)
        with open(filename, 'rb') as f:
            res.setdefault('url', 'unused-but-required')
            return self._call_action('resource_update', res, files={'upload': f})

    def _call_action(self, method_name, data, **kwargs):
        return self.api.call_action(method_name, data, **kwargs)

    def sync(self, master):
        '''
        ``master`` is a list of dataset descriptions.
        '''
        master_pkgs = {pkg['name']: pkg for pkg in self._check_master(master)}

        # Find all existing datasets that belong to the importer
        existing_pkgs = self._find_pkgs_by_extra('ckanext_importer_importer_id', solr_escape(self.id))
        log.debug('Found {} existing package(s)'.format(len(existing_pkgs)))
        #log.debug([pkg['name'] for pkg in existing_pkgs])

        # For each existing dataset: if it has a master, update it, otherwise remove it
        for existing_pkg in existing_pkgs:
            try:
                master_pkg = master_pkgs.pop(existing_pkg['name'])
            except KeyError:
                self._purge_pkg(existing_pkg)
                continue
            self._sync_pkg(existing_pkg, master_pkg)

        # For the remaining master datasets: create real datasets
        for master_pkg in master_pkgs.values():
            self._create_pkg(master_pkg)

    def _check_master(self, master):
        with_name = [pkg for pkg in master if 'name' in pkg]
        if len(with_name) != len(master):
            log.warning('Ignoring {} dataset(s) without "name" attribute'.format(len(master) - len(with_name)))
        with_org = []
        for pkg in with_name:
            if 'owner_org' in pkg:
                with_org.append(pkg)
            else:
                log.warning('Ignoring master "{}" because it has no "owner_org" attribute'.format(pkg['name']))
        return with_org

    def _sync_pkg(self, existing, master):
        '''
        Synch a single dataset.

        ``existing`` is the package dict of an existing dataset.

        ``master`` is the description for that dataset.
        '''
        name = master['name']
        log.info('Synchronizing dataset {}'.format(name))

        updated = copy.deepcopy(existing)
        special_keys = {'owner_org', 'resources'}
        updated.update((key, value) for key, value in master.items()
                       if key not in special_keys)

        # Make sure the importer ID is listed in the extras, it might
        # have been overwritten if the master supplied its own extras.
        _set_extra(updated, 'ckanext_importer_importer_id', self.id)
        # CKAN sorts extras by their key, so we need to also do that.
        # Otherwise the difference check further down returns false
        # positives.
        updated['extras'].sort(key=lambda extra: extra['key'])

        org = updated['organization']
        if master['owner_org'] not in (org['id'], org['name']):
            log.debug('Organisation of dataset {} changed'.format(name))
            del updated['organization']
            updated['owner_org'] = master['owner_org']

        file_uploads = {}

        updated_resources = []
        for res_num, (existing_res, master_res) in enumerate(izip_longest(updated['resources'], master.get('resources', []))):
            if existing_res is None:
                # Additional resources in master
                log.debug('Additional resource #{} in master for dataset {}'.format(res_num, name))

                special_keys = {'_file'}
                new_res = {key: value for key, value in master_res.items()
                           if key not in special_keys}

                if '_file' in master_res:
                    file_uploads[res_num] = master_res['_file']
                    new_res.setdefault('url', 'unused-but-required')

                updated_resources.append(new_res)
            elif master_res is None:
                # Resources have been removed in master
                log.debug('Removing resource #{} from dataset {} because it is missing from master'.format(
                          res_num, name))
                pass
            else:
                updated_res = copy.deepcopy(existing_res)

                special_keys = {'_file'}
                updated_res.update((key, value) for key, value in master_res.items()
                                   if key not in special_keys)

                if '_file' in master_res:
                    file_uploads[res_num] = master_res['_file']

                if updated_res != existing_res:
                    log.debug('Resource #{} of dataset {} has changed'.format(res_num, name))
                updated_resources.append(updated_res)

        updated['resources'] = updated_resources

        if existing != updated:
            log.debug('Dataset {} has changed'.format(name))
            log.debug(jsondiff.diff(existing, updated))
            #log.debug(existing)
            #log.debug(updated)
            updated = self._call_action('package_update', updated)
        else:
            log.debug('Dataset {} has not changed'.format(name))

        for res_num, filename in file_uploads.items():
            log.debug('Uploading file "{}" to resource #{} of dataset {}'.format(filename, res_num, name))
            # We need to make sure to use the package dict returned by CKAN after a potential
            # update here, because resource masters don't have their `id` and `package_id` set.
            self._upload_resource_file(filename, updated['resources'][res_num])


    def _create_pkg(self, master):
        '''
        Create a single dataset.

        ``master`` is the description for that dataset.

        CKAN errors are logged and swallowed.
        '''
        name = master['name']
        log.info('Creating new dataset {}'.format(name))

        special_keys = {'resources'}
        new_pkg = {key: copy.deepcopy(value) for key, value in master.items()
                   if key not in special_keys}

        _set_extra(new_pkg, 'ckanext_importer_importer_id', self.id)

        file_uploads = {}

        new_pkg['resources'] = new_resources = []
        for i, res in enumerate(master.get('resources', [])):
            special_keys = {'_file'}
            new_res = {key: copy.deepcopy(value) for key, value in res.items()
                       if key not in special_keys}

            if '_file' in res:
                file_uploads[i] = res['_file']
                new_res.setdefault('url', 'unused-but-required')

            new_resources.append(new_res)

        try:
            new_pkg = self._call_action('package_create', new_pkg)
        except Exception as e:
            log.error('Error while creating new dataset {}: {}'.format(name, e))
            return

        for res_num, filename in file_uploads.items():
            log.debug('Uploading file "{}" to resource #{} of dataset {}'.format(filename, res_num, name))
            self._upload_resource_file(filename, new_pkg['resources'][res_num])


    def _purge_pkg(self, existing):
        '''
        Purge an existing CKAN dataset.

        ``existing`` is the package dict.
        '''
        log.info('Purging dataset {}'.format(existing['name']))
        self._call_action('dataset_purge', {'id': existing['id']})

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
        return self._call_action('package_search', {'fq':fq, 'rows': '1000'})['results']


if __name__ == '__main__':
    import io
    import json

    from ckanapi import RemoteCKAN

    log.addHandler(logging.StreamHandler())
    log.setLevel(logging.DEBUG)

    master = [
        {
            'title': 'No name to test the warning',
        },
        {
            'name': 'no-owner_org-to-test-the-warning',
        },
        {
            'name': 'a-first-test',
            'owner_org': 'stadt-karlsruhe',
            'title': 'We have a title!',
        },
        {
            'name': 'a-test-with-extras',
            'extras': [
                {'key': 'something', 'value': 'special?'},
            ],
            'owner_org': 'stadt-karlsruhe',
        },
        {
            'name': 'a-test-with-an-organization-id-instead-of-name',
            'owner_org': '12d5f4e4-036e-49de-84ef-5a8d617a3f9b',
        },
        {
            'name': 'a-test-with-resources',
            'owner_org': 'stadt-karlsruhe',
            'resources': [
                {
                    'name': 'A new name',
                    'url': 'https://some-url2',
                },
                {
                    'url': 'https://a-url',
                },
            ],
        },
        {
            'name': 'a-resource-with-a-file-upload',
            'owner_org': 'stadt-karlsruhe',
            'resources': [
                {
                    '_file': 'test1.csv',
                },
            ],
        }
    ]

    with io.open('apikey.json', 'r', encoding='utf-8') as f:
        apikey = json.load(f)['apikey']

    with RemoteCKAN('https://test-transparenz.karlsruhe.de', apikey=apikey) as api:
        importer = Importer('test-importer-01', api)
        importer.sync(master)

