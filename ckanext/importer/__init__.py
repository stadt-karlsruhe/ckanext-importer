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

from copy import deepcopy
import collections
from enum import Enum
from itertools import islice
import json
import logging

import ckanapi
from ckan.logic import NotFound

from .utils import (DictWrapper, context_manager_method, replace_dict,
                    solr_escape)


__all__ = ['Importer', 'OnError']


__version__ = '0.1.0'


class Entity(DictWrapper):
    '''
    Base class for package, resource, and view wrappers.

    Not to be instantiated directly.
    '''
    def __init__(self, eid, data_dict, parent):
        '''
        Constructor.

        ``data_dict`` is the dict wrapped by this entity.

        ``parent`` is the parent entity.

        ``imp`` is the importer to which this entity belongs. Defaults
        to the parent's importer.
        '''
        super(Entity, self).__init__(data_dict)
        self._eid = eid
        self._parent = parent
        self._api = parent._api
        self._log = parent._log
        self._mark_as_unmodified()
        self._to_be_deleted = False
        self._synced_child_eids = set()

    def _mark_as_unmodified(self):
        '''
        Mark this entity as unmodified.
        '''
        self._original_dict = deepcopy(self._dict)

    def _is_modified(self):
        '''
        Check if this entity has been modified.
        '''
        return self._original_dict != self._dict

    def delete(self):
        '''
        Mark this entity for deletion.
        '''
        self._to_be_deleted = True

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

    def _mark_as_synced(self):
        '''
        Mark this entity as synced in the parent entity.
        '''
        self._parent._synced_child_eids.add(self._eid)

    def __repr__(self):
        try:
            id_part = 'id={!r} '.format(self['id'])
        except KeyError:
            id_part = ''
        return '<{} {}eid={!r}>'.format(self.__class__.__name__,
                                        id_part, self._eid)


class OnError(Enum):
    '''
    Error handling constants.

    Used for the ``on_error`` argument of
    :py:meth:`.Importer.sync_package`,
    :py:meth:`.Package.sync_resource`, and
    :py:meth:`.Resource.sync_view`.
    '''
    #: Reraise the exception. If the entity was created at the beginning
    #: of the current context manager (i.e. if no entity for that EID
    #: existed before) then that entity is deleted before the exception
    #: is reraised.
    reraise = 1

    #: Swallow the exception and keep the old version of the entity. If
    #: the entity was created at the beginning of the current context
    #: manager (i.e. if no entity for that EID existed before) then that
    #: entity is not kept.
    keep = 2

    #: Swallow the exception and delete the entity.
    delete = 3


class EntitySyncManager(object):
    '''
    Context manager for synchronizing an ``Entity``.

    Do not instantiate directly.
    '''
    def __init__(self, eid, on_error=OnError.reraise):
        self._eid = unicode(eid)
        if not isinstance(on_error, OnError):
            raise TypeError('on_error must be of type OnError')
        self._on_error = on_error

    def _find_entity(self):
        '''
        Find an existing entity.

        Subclasses must implement this method to return an existing
        entity based on ``_eid``.

        If no entity with that EID exists then ``ckan.logic.NotFound``
        must be raised.
        '''
        raise NotImplementedError()

    def _create_entity(self):
        '''
        Create a new entity.

        Subclasses must implement this method to create and return a
        new entity based on ``_eid``.
        '''
        raise NotImplementedError

    def __enter__(self):
        try:
            self._entity = self._find_entity()
            self._just_created = False
            self._outer._log.debug('Using {}'.format(self._entity))
        except NotFound:
            self._entity = self._create_entity()
            self._just_created = True
            self._outer._log.debug('Created {}'.format(self._entity))
        assert self._entity is not None
        self._entity._mark_as_synced()
        return self._entity

    def __exit__(self, exc_type, exc_val, exc_tb):
        entity = self._entity

        def delete():
            try:
                entity._delete()
            except Exception as e:
                entity._log.exception('Error while deleting {}: {}'.format(entity, e))
                if self._on_error == OnError.reraise:
                    raise

        if exc_type is not None:
            if self._just_created:
                # If the entity was created at the beginning of the context
                # manager then it is deleted regardless of the on_error
                # setting
                entity._log.error('Newly created {} will not be kept due to an error: {}'.format(entity, exc_val),
                                  exc_info=(exc_type, exc_val, exc_tb))
                delete()
            elif self._on_error == OnError.delete:
                entity._log.error('Deleting existing {} due to an error: {}'.format(entity, exc_val),
                                  exc_info=(exc_type, exc_val, exc_tb))
                delete()
            else:
                # OnError.keep and OnError.reraise
                entity._log.error('Changes to {} will not be uploaded due to an error: {}'.format(entity, exc_val),
                                  exc_info=(exc_type, exc_val, exc_tb))
            return self._on_error != OnError.reraise  # Swallow/reraise
        if entity._to_be_deleted:
            entity._log.debug('Deleting {}'.format(entity))
            delete()
        elif entity._is_modified():
            entity._log.debug('Uploading {}'.format(entity))
            try:
                entity._upload()
            except Exception as e:
                entity._log.exception('Error while uploading {}: {}'.format(entity, e))
                if self._just_created:
                    entity._log.error('Newly created {} will not be kept after failed upload'.format(entity))
                    delete()
                elif self._on_error == OnError.delete:
                    entity._log.error('Deleting {} after failed upload'.format(entity))
                    delete()
                if self._on_error == OnError.reraise:
                    raise
        else:
            entity._log.debug('{} has not been modified'.format(entity))


_PACKAGE_NAME_PREFIX = 'ckanext_importer_'


def _search_packages(api, **kwargs):
    '''
    Wrapper around CKAN's ``package_search`` to handle pagination.

    Passes on all keyword arguments to ``package_search`` and yields the
    found package dicts.

    Transparently handles pagination. You may set the ``rows`` argument
    to control how many results are returned per call of
    ``package_search``. Note, however, that the ``start`` argument of
    ``package_search`` is automatically set by this function.
    '''
    kwargs['start'] = 0
    while True:
        result = api.action.package_search(**kwargs)
        for pkg_dict in result['results']:
            yield pkg_dict
        num_retrieved = len(result['results'])
        if kwargs['start'] + num_retrieved == result['count']:
            # All results have been retrieved
            break
        kwargs['start'] += num_retrieved


class Importer(object):
    '''
    An importer.

    This class allows you to sync packages (and, from there, resources
    and views) between an external data source and CKAN.

    ``id`` is the ID of this importer. The ID needs to be unique among
    all importers used on this CKAN instance. It is converted to Unicode
    automatically.

    ``api`` is an optional instance of ``ckanapi.LocalCKAN`` or
    ``ckanapi.RemoteCKAN`` and provides the CKAN instance with to sync
    data with. If not given it defaults to ``ckanapi.LocalCKAN``, i.e.
    the currently running local CKAN instance.

    ``default_owner_org`` is the default setting for the ``owner_org``
    field of packages created via :py:meth:`.sync_package` and can be
    either the name or the ID of an existing CKAN organization.

    .. automethod:: sync_package(eid, on_error=OnError.reraise)

       Sync a package.

       This is a context manager that returns a :py:class:`Package`
       instance for the CKAN package corresponding to the given EID.
       The package can then be modified inside the context manager. Once
       the context manager exits, the modified package is uploaded to
       CKAN.

       If no package exists for the given EID then one is created.

       If the package is not modified inside the context manager then it
       is not re-uploaded to CKAN.

       `on_error` is an instance of :py:class:`OnError` and controls how
       exceptions inside the context manager are handled.
    '''

    class _PrefixLoggerAdapter(logging.LoggerAdapter):
        '''
        A LoggerAdapter that adds a prefix to each message.
        '''
        def __init__(self, logger, prefix):
            super(Importer._PrefixLoggerAdapter, self).__init__(
                logger, {'prefix': prefix})

        def process(self, msg, kwargs):
            return self.extra['prefix'] + msg, kwargs

    def __init__(self, id, api=None, default_owner_org=None):
        self.id = unicode(id)
        self._api = api or ckanapi.LocalCKAN()
        self.default_owner_org = default_owner_org
        self._synced_child_eids = set()
        self._log = Importer._PrefixLoggerAdapter(
            logging.getLogger(__name__), 'Importer {!r}: '.format(self.id))

    def delete_unsynced_packages(self):
        '''
        Delete packages that have not been synced.

        This method deletes all packages belonging to this importer for
        which :py:meth:`.sync_package` has not been called since this
        :py:class:`Importer` instance has been created.

        It is intended to be called after all desired packages have been
        synced to delete those CKAN packages corresponding to objects
        that have been removed from the data source since the last
        import.
        '''
        for pkg_dict in self._find_packages():
            extras = ExtrasDictView(pkg_dict['extras'])
            eid = extras['ckanext_importer_package_eid']
            if eid not in self._synced_child_eids:
                pkg = Package(eid, pkg_dict, self)
                self._log.debug('Deleting unsynced {}'.format(pkg))
                pkg._delete()

    @context_manager_method
    class sync_package(EntitySyncManager):
        # Documentation is in the class docstring
        def _find_entity(self):
            pkg_dict = self._outer._find_package(self._eid)
            return Package(self._eid, pkg_dict, self._outer)

        def _create_entity(self):
            i = 0
            while True:
                name = '{}{}'.format(_PACKAGE_NAME_PREFIX, i)
                try:
                    pkg_dict = self._outer._api.action.package_create(
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
                return Package(self._eid, pkg_dict, self._outer)

    def _find_packages(self, eid=None):
        '''
        Find existing packages for this importer.

        Yields package dicts.

        If ``eid`` is given, then only packages with that EID are returned.
        '''
        extras = {
            'ckanext_importer_importer_id': solr_escape(self.id),
        }
        if eid is not None:
            extras['ckanext_importer_package_eid'] = solr_escape(eid)
        fq = ' AND '.join('extras_{}:"{}"'.format(*item)
                          for item in extras.items())
        pkg_dicts = _search_packages(self._api, fq=fq, rows=1000,
                                     include_private=True)

        # CKAN's search is based on Solr, which by default doesn't support
        # searching for exact matches. Hence searching for importer ID "x"
        # can also return packages with importer ID "x-y". Hence we filter
        # the results again.
        for pkg_dict in pkg_dicts:
            extras = ExtrasDictView(pkg_dict['extras'])
            if extras['ckanext_importer_importer_id'] != self.id:
                continue
            if eid is not None and extras['ckanext_importer_package_eid'] != eid:
                continue
            yield pkg_dict

    def _find_package(self, eid):
        '''
        Find an existing package for this importer.

        ``eid`` is the EID of the package.

        Returns the package dict.

        Raises ``ckan.logic.NotFound`` if no package with that EID could
        be found.

        Raises ``RuntimeError`` if more than one package with the given
        EID are found. This only happens with a corrupted database.
        '''
        pkg_dicts = list(islice(self._find_packages(eid), 2))
        if not pkg_dicts:
            raise NotFound('No package with EID {!r} exists for {}'.format(eid, self))
        if len(pkg_dicts) > 1:
            raise RuntimeError('Multiple packages with EID {!r} found for {}'.format(eid, self))
        return pkg_dicts[0]

    def __repr__(self):
        return '<{} id={!r}>'.format(self.__class__.__name__, self.id)


class Package(Entity):
    '''
    Wrapper around a CKAN package dict.

    Not to be instantiated directly. Use
    :py:meth:`Importer.sync_package` instead.

    The package can be modified using the standard `dict`-interface::

        with imp.sync_package('my-eid') as pkg:
            pkg['title'] = 'A new title'

    .. automethod:: sync_resource(eid, on_error=OnError.reraise)

       Sync a resource of this package.

       This is a context manager that returns a :py:class:`Resource`
       instance for the package's CKAN resource corresponding to the
       given EID. The resource can then be modified inside the context
       manager. Once the context manager exits, the modified resource is
       uploaded to CKAN.

       If no resource exists for the given EID then one is created.

       If the resource is not modified inside the context manager then
       it is not re-uploaded to CKAN.

       `on_error` is an instance of :py:class:`OnError` and controls how
       exceptions inside the context manager are handled.
    '''
    def __init__(self, eid, pkg_dict, parent):
        super(Package, self).__init__(eid, pkg_dict, parent)

        #: `dict`-interface for package extras.
        #:
        #: CKAN stores package extras as a list of key/value dicts,
        #: which makes modifying them cumbersome. This attribute allows
        #: you to access the extras like a regular `dict` instead::
        #:
        #:     with imp.sync_package('my-eid') as pkg:
        #:         pkg.extras['my-extra'] = 'some value'
        #:
        #: Reading an extra returns the value of the first extra with
        #: the given key or raises a `KeyError` if no extra with that
        #: key exists.
        #:
        #: Writing an extra overwrites the value of the first extra with
        #: the given key or appends a new extra at the end of the extras
        #: list if no extra with the given key exists.
        #:
        #: Deleting an extra deletes the first extra with the given key
        #: or raises a `KeyError` when no extra with that key exists.
        #:
        #: If you need more control regarding extras with duplicate keys
        #: and the order of extras then you need to manage extras
        #: manually (using `pkg['extras']` instead of `pkg.extras`).
        self.extras = ExtrasDictView(pkg_dict['extras'])

    def _upload(self):
        replace_dict(self,
                     self._api.action.package_update(**self))

    def _delete(self):
        '''
        Purge this package.
        '''
        self._api.action.dataset_purge(id=self['id'])

    def delete_unsynced_resources(self):
        '''
        Delete resources that have not been synced.

        This method deletes all resources belonging to this package for
        which :py:meth:`.sync_resource` has not been called since this
        :py:class:`Package` instance has been created.

        It is intended to be called after all desired resources of this
        package have been synced to delete those CKAN resources
        corresponding to objects that have been removed from the data
        source since the last import.
        '''
        for res_dict in list(self['resources']):
            eid = res_dict['ckanext_importer_resource_eid']
            if eid not in self._synced_child_eids:
                res = Resource(eid, res_dict, self)
                self._log.debug('Deleting unsynced {}'.format(res))
                res._delete()

    @context_manager_method
    class sync_resource(EntitySyncManager):
        # Documentation is in the class docstring
        def _find_entity(self):
            res_dicts = [r for r in self._outer['resources']
                         if r['ckanext_importer_resource_eid'] == self._eid]
            if not res_dicts:
                raise NotFound('No resource with EID {!r} in {}'.format(self._eid, self._outer))
            if len(res_dicts) > 1:
                raise ValueError('Multiple resources for EID {} in {}'.format(self._eid, self._outer))
            return Resource(self._eid, res_dicts[0], self._outer)

        def _create_entity(self):
            res_dict = self._outer._api.action.resource_create(
                package_id=self._outer['id'],
                ckanext_importer_resource_eid=self._eid,
            )
            self._outer['resources'].append(res_dict)
            return Resource(self._eid, res_dict, self._outer)

        def __enter__(self):
            self._pkg_is_modified = self._outer._is_modified()
            # Note: This call should use super(), but see https://stackoverflow.com/q/51860397/857390
            return EntitySyncManager.__enter__(self)

        def __exit__(self, exc_type, exc_val, exc_tb):
            # Note: This call should use super(), but see https://stackoverflow.com/q/51860397/857390
            result = EntitySyncManager.__exit__(self, exc_type, exc_val, exc_tb)
            if not self._pkg_is_modified:
                # If the package was previously unmodified we mark it
                # as unmodified again, since changes in the resource
                # have already been uploaded (but their propagation to
                # the package dict has marked the package as modified).
                self._outer._mark_as_unmodified()
            return result

class Resource(Entity):
    '''
    Wrapper around a CKAN resource dict.

    Do not instantiate directly, use
    :py:meth:`Package.sync_resource` instead.

    The resource can be modified using the standard `dict`-interface::

        with pkg.sync_resource('my-eid') as res:
            res['name'] = 'A new name'

    .. automethod:: sync_view(eid, on_error=OnError.reraise)

       Sync a view of this resource.

       This is a context manager that returns a :py:class:`View`
       instance for the resource's CKAN view corresponding to the given
       EID. The view can then be modified inside the context manager.
       Once the context manager exits, the modified resource is uploaded
       to CKAN.

       If no view exists for the given EID then one is created.

       If the view is not modified inside the context manager then
       it is not re-uploaded to CKAN.

       `on_error` is an instance of :py:class:`OnError` and controls how
       exceptions inside the context manager are handled.
    '''
    def _delete(self):
        id = self['id']
        self._api.action.resource_delete(id=id)
        self._parent['resources'][:] = [r for r in self._parent['resources']
                                        if r['id'] != id]

    def _upload(self):
        '''
        Upload the modified resource dict and propagate the changes.
        '''
        replace_dict(self, self._api.action.resource_update(**self))

    def _get_views_map(self):
        '''
        Get the map of views for this resource.

        The views map maps view EIDs to view IDs.
        '''
        return json.loads(self.get('ckanext_importer_views', '{}'))

    def _set_views_map(self, views):
        '''
        Set the map of views for this resource.
        '''
        self['ckanext_importer_views'] = json.dumps(views, separators=(',', ':'))

    def delete_unsynced_views(self):
        '''
        Delete views that have not been synced.

        This method deletes all views belonging to this resource for
        which :py:meth:`.sync_view` has not been called since this
        :py:class:`Resource` instance has been created.

        It is intended to be called after all desired views of this
        resource have been synced to delete those CKAN views that are no
        longer desired.
        '''
        for eid, id in list(self._get_views_map().items()):
            if eid not in self._synced_child_eids:
                view = View(eid, {'id': id}, self)
                self._log.debug('Deleting unsynced {}'.format(view))
                view._delete()

    @context_manager_method
    class sync_view(EntitySyncManager):
        # Documentation is in the class docstring
        def _find_entity(self):
            views = self._outer._get_views_map()
            try:
                id = views[self._eid]
            except KeyError:
                raise NotFound('No view with EID {!r} in {}'.format(self._eid, self._outer))
            view_dict = self._outer._api.action.resource_view_show(id=id)
            return View(self._eid, view_dict, self._outer)

        def _create_entity(self):
            return View(self._eid, {}, self._outer)


class View(Entity):
    '''
    Wrapper around a CKAN view.

    Do not instantiate directly. Use
    :py:meth:`Resource.sync_view` instead.

    The view can be modified using the standard `dict`-interface::

        with res.sync_view('my-eid') as view:
            view['title'] = 'A new title'
    '''

    # FIXME: When we update the resource's ckanext_importer_views field
    #        then the upstream res dict is only updated once the
    #        sync_resource CM exits. If this doesn't happen (due
    #        to an exception or a call to dont_sync) then we end
    #        up with an already created/deleted view that isn't
    #        properly tracked by ckanext.importer. At the very least we
    #        should automatically discover such issues, better would be
    #        to prevent them in the first place.

    def _upload(self):
        try:
            id = self['id']
        except KeyError:
            self._create()
        else:
            replace_dict(self, self._api.action.resource_view_update(**self))

    def _create(self):
        '''
        Create a view.
        '''
        self['resource_id'] = self._parent['id']
        replace_dict(self, self._api.action.resource_view_create(**self))
        # Register the view in the resource
        views = self._parent._get_views_map()
        views[self._eid] = self['id']
        self._parent._set_views_map(views)

    def _delete(self):
        try:
            id = self['id']
        except KeyError:
            # View has not been created yet
            return
        self._api.action.resource_view_delete(id=id)
        # Unregister the view in the resource
        views = self._parent._get_views_map()
        del views[self._eid]
        self._parent._set_views_map(views)


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
