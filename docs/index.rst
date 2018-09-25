.. py:currentmodule:: ckanext.importer

Overview
========

*ckanext.importer* provides utilities for easily importing metadata
from an external data source into CKAN and keeping the CKAN metadata
up-to-date when the contents of the data source is modified.

To achieve this, each entity (package, resource, view) in CKAN is linked
to its counterpart in the original data source via an **external ID**
(**EID**), for example the entity's ID in the data source.

As an example, let's create a package with a resource::

    from ckanext.importer import Importer

    imp = Importer('my-importer-id')

    with imp.sync_package('my-package-eid') as pkg:
        # If no package with the given EID exists then it is
        # automatically created. Otherwise the existing package
        # is retrieved.

        # The package can be modified like a dict
        pkg['title'] = 'My Package Title'

        # For package extras there's the special `.extras` attribute
        # which provides a dict-interface:
        pkg.extras['my-extra-key'] = 'my-extra-value'

        with pkg.sync_resource('my-resource-eid') as res:
            # Just like packages, resources are automatically created
            # and retrieved based on their EID.
            res['name'] = 'My Resource Name'

        # Once the `sync_resource` context manager exists, the
        # created/updated resource is automatically uploaded to CKAN.

    # Once the `sync_package` context manager exists, the created/updated
    # package is automatically uploaded to CKAN.

For more details on how to use *ckanext.importer* please refer to Usage_.


Installation
============

*ckanext.importer* uses the usual installation routine for CKAN extensions:

1. Activate your CKAN virtualenv:

   .. code-block:: bash

       cd /usr/lib/ckan/default
       source bin/activate

2. Install *ckanext.importer* and its dependencies:

   .. code-block:: bash

       pip install -e git+https://github.com/stadt-karlsruhe/ckanext-importer#egg=ckanext-importer
       pip install -r src/ckanext-importer/requirements.txt

   On a production system you'll probably want to pin a certain `release version`_ of *ckanext.importer* instead:

   .. code-block:: bash

       pip install -e git+https://github.com/stadt-karlsruhe/ckanext-importer@v0.1.0#egg=ckanext-importer

3. Restart CKAN. For example, if you're using Apache,

   .. code-block:: bash

       sudo service apache2 restart

.. _release version:  https://github.com/stadt-karlsruhe/ckanext-importer/releases


Usage
=====
*ckanext.importer* provides utilities to write Python code for importing
and synchronizing CKAN metadata from an external data source.

.. note::
    At this point in time, *ckanext.importer* does *not* provide a web
    UI or any command line tools.

The starting point for using *ckanext.importer* is an
:py:class:`Importer`. Each :py:class:`Importer` instance corresponds to
a separate data source and is identified using an ID that can be freely
chosen (but must be unique among all importers used on the target CKAN
instance)::

    from ckanext.importer import Importer

    imp = Importer('my-importer-id')

Once you have created an importer, you use its
:py:meth:`~Importer.sync_package` method to create/update the CKAN
metadata for a dataset. The CKAN package is linked to your external
dataset using an external ID (EID). *ckanext.importer* automatically
stores the EID along with the other package metadata inside CKAN. Like
the importer ID, the package's EID can be chosen freely, but must be
unique among all packages for this importer.

.. code-block:: python

    with imp.sync_package(eid='my-package-eid') as pkg:
        # ckanext.importer has automatically checked whether a
        # package for this importer ID and package EID already
        # exists and -- if that is the case -- retrieved it.
        # Otherwise, a suitable package has been automatically
        # created for you.

        # Use the package's dict-interface to insert/update the
        # metadata from your data source. For example:
        pkg['title'] = 'My Package Title'

    # Once the context manager exits, the modified package is
    # automatically uploaded to CKAN.

Typically, you don't have only one dataset, but an external data source
(for example a database) containing many datasets to be imported::

    for external_dataset in external_datasource:
        with imp.sync_package(eid=external_dataset.id) as pkg:
            pkg['title'] = external_dataset.name

Synchronizing a package's resources works pretty much the same: the
object returned by :py:meth:`~Importer.sync_package` is an instance
of :py:class:`Package` and provides a :py:meth:`~Package.sync_resource`
method::

    with imp.sync_package(eid='my-package-eid') as pkg:
        pkg['title'] = 'My Package Title'

        with pkg.sync_resource(eid='my-resource-eid') as res:
            res['name'] = 'My Resource Name'
            res['url'] = 'https://some-resource-url'

Resource EIDs need to be unique among all resources of the same package.

Finally, the same mechanism can be used to synchronize resource views
via the :py:meth:`Resource.sync_view` method (which returns a
:py:class:`View` instance)::

    with pkg.sync_resource(eid='my-resource-eid') as res:
        res['name'] = 'My Resource Name'
        res['url'] = 'https://some-resource-url'

        with res.sync_view(eid='my-view-eid') as view:
            view['view_type'] = 'text_view'
            view['title'] = 'My View Title'

See the `API Reference`_ for more information.


Error Handling
--------------
A main design principle of *ckanext.importer* is to keep CKAN's version of the
imported data in a well-defined state in case of an error. To support different
use cases, there are different approaches to error handling, which can be
configured using the ``on_error`` argument of :py:meth:`Importer.sync_package`,
:py:meth:`Package.sync_resource`, and :py:meth:`Resource.sync_view`:

  - **Re-raise the exception** (:py:attr:`OnError.reraise`): If an exception
    occurs inside the context manager, log and re-raise it.

    Changes made inside the context manager are not uploaded to CKAN. The
    previous state of the entity is kept in CKAN. If a new entity was created
    at the beginning of the context manager then it is deleted

    This is the default behavior.

  - **Swallow the exception and keep the entity** (:py:attr:`OnError.keep`):
    The exception is logged, but not re-raised.

  - **Swallow the exception and delete the entity**
    (:py:attr:`OnError.delete`): The exception is logged, but not re-raised.
    The entity is deleted from CKAN.


License
=======

Copyright © 2018, `Stadt Karlsruhe`_.

Distributed under the GNU Affero General Public License. See the file
LICENSE_ for details.

.. _Stadt Karlsruhe: https://www.karlsruhe.de

.. _LICENSE: https://github.com/stadt-karlsruhe/ckanext-importer/blob/master/LICENSE


Changelog
==========

See the file CHANGELOG.md_.

.. _CHANGELOG.md: https://github.com/stadt-karlsruhe/ckanext-importer/blob/master/CHANGELOG.md


API Reference
=============

.. automodule:: ckanext.importer
    :members:
    :undoc-members:
    :ignore-module-all:
    :exclude-members: Entity, EntitySyncManager, ExtrasDictView,
                      sync_package, sync_resource, sync_view

