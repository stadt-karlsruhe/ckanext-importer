#!/usr/bin/env python

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# See conftest.py for the definition of the pytest fixtures


def test_package_creation(api, imp):
    '''
    Test creation of a package.
    '''
    title = 'Hello, world!'
    with imp.sync_package('x') as pkg:
        # Check that the package already exists
        id = pkg['id']
        api.action.package_show(id=id)

        pkg['title'] = title

    # Check that the changes have been synced
    assert api.action.package_show(id=id)['title'] == title


def test_package_update(api, imp):
    '''
    Test update of a package.
    '''
    for i in range(3):
        title = str(i)
        with imp.sync_package('y') as pkg:
            pkg['title'] = title
            id = pkg['id']
        assert api.action.package_show(id=id)['title'] == title


def test_different_importer_ids_same_package_eid(imp_factory):
    '''
    Test that different importers can use the same package EID.
    '''
    imp1 = imp_factory(id='test-importer-1')
    imp2 = imp_factory(id='test-importer-2')
    eid = 'shared-eid'
    with imp1.sync_package(eid) as pkg:
        pkg_id1 = pkg['id']
    with imp2.sync_package(eid) as pkg:
        pkg_id2 = pkg['id']
    assert pkg_id1 != pkg_id2
