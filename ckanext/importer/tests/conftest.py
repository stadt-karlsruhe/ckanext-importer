#!/usr/bin/env python

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

