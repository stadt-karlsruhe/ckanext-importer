#!/usr/bin/env python2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)




class Manager(object):

    def __init__(self, *args, **kwargs):
        print('Manager.init')
        print('  args = {}'.format(args))
        print('  kwargs = {}'.format(kwargs))

    def __enter__(self):
        print('Manager.__enter__')

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('Manager.__exit__')
        print('  exc_type = {}'.format(exc_type))
        print('  exc_val = {}'.format(exc_val))
        print('  exc_tb = {}'.format(exc_tb))

        if exc_type is not None:
            # An exception occurred
            print('An exception occurred')
            print('Swallowing that exception')
            return True  # Swallow exception


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


class nested_cm_method(object):

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


class Importer(object):

    def __init__(self, id):
        self.id = id
        self._synced_pkg_ids = []
        self._active = False

    def __enter__(self):
        self._active = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._active = False
        if exc_type is not None:
            print('An exception occurred during package synchronization')
            print('Not removing existing packages')
            print('But swallowing the exception (after logging it)')
            print(exc_val)
            return
        print('Removing all existing but unsynced packages')
        existing_pkg_ids = set(self._get_existing_pkgs().values())
        unsynced_pkg_ids = existing_pkg_ids.difference(self._synced_pkg_ids)
        for pkg_id in unsynced_pkg_ids:
            print('Removing existing but unsynced package "{}"'.format(pkg_id))

    def _get_existing_pkgs(self):
        '''
        Get the existing CKAN packages for this importer.

        Finds all existing CKAN packages belonging to this importer
        (based on the importer ID). Returns a dict that maps their
        master ID to their CKAN ID.

        Packages that have an importer ID but no master ID are removed
        from CKAN.
        '''
        return {}

    @nested_cm_method
    class sync_package(object):
        '''
        This docstring is passed on to the method
        '''
        def __init__(self, *args, **kwargs):
            print('SyncPackage.__init__')
            print('  args = {}'.format(args))
            print('  kwargs = {}'.format(kwargs))
            print('  _outer = {}'.format(self._outer))

        def __enter__(self):
            print('SyncPackage.__enter__')
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            print('SyncPackage.__exit__')


if __name__ == '__main__':
    print('Before with')
    with Manager('foo', bar='bar') as manager:
        print('Begin of inside with')
        print('manager = {}'.format(manager))

        print('Before exception')
        raise ValueError('Oh no!')
        print('After exception')

        print('End of inside with')
    print('After with')

