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

import errno
import io
import json
import logging
import os.path

import paste.deploy
from paste.registry import Registry
from pylons import config, translator
import pytest

from ckan.config.environment import load_environment
from ckan.lib.cli import MockTranslator
import ckan.lib.search
from ckan.tests.helpers import reset_db
import ckanapi

import ckanext.importer


HERE = os.path.abspath(os.path.dirname(__file__))

TEST_INI = os.path.join(HERE, '..', '..', '..', 'test.ini')

IMPORTER_ID = 'test-importer'


# Adapted from ckanext-archiver
def _load_ckan_environment(ini_path):
    '''
    Load CKAN environment.
    '''
    ini_path = os.path.abspath(ini_path)
    logging.config.fileConfig(ini_path, disable_existing_loggers=False)
    conf = paste.deploy.appconfig('config:' + ini_path)
    load_environment(conf.global_conf, conf.local_conf)
    _register_translator()


# Adapted from ckanext-archiver
def _register_translator():
    '''
    Register a translator in this thread.
    '''
    global registry
    try:
        registry
    except NameError:
        registry = Registry()
    registry.prepare()
    global translator_obj
    try:
        translator_obj
    except NameError:
        translator_obj = MockTranslator()
    registry.register(translator, translator_obj)


def _rebuild_search_index():
    '''
    Rebuild CKAN's Solr search index.
    '''
    ckan.lib.search.rebuild(defer_commit=True)
    ckan.lib.search.commit()


@pytest.fixture(scope='session')
def api():
    '''
    CKAN API fixture.

    Initializes the CKAN environment and returns a ``ckanapi.LocalCKAN``
    instance.
    '''
    _load_ckan_environment(TEST_INI)
    return ckanapi.LocalCKAN()


@pytest.fixture
def imp_factory(api):
    '''
    Importer factory fixture.

    Yields a factory for ``ckanext.importer.Importer`` instances.

    The factory takes the same arguments as ``Importer``, but provides
    default values for the ``id`` and ``api`` arguments.

    Once the test case is finished, the CKAN DB is reset and the search
    index is rebuilt.
    '''
    def importer(id=IMPORTER_ID, *args, **kwargs):
        kwargs['api'] = api
        return ckanext.importer.Importer(id, *args, **kwargs)

    yield importer

    reset_db()
    _rebuild_search_index()


@pytest.fixture
def imp(imp_factory):
    '''
    Importer fixture.

    Yields a ``ckanext.importer.Importer`` instance.

    Once the test case is finished, the CKAN DB is reset and the search
    index is rebuilt.
    '''
    return imp_factory()


@pytest.fixture
def pkg(imp):
    '''
    Package fixture.

    Yields a ``ckanext.importer.Package`` instance.

    Once the test case is finished, the CKAN DB is reset and the search
    index is rebuilt.
    '''
    with imp.sync_package('test-package-eid') as package:
        yield package
