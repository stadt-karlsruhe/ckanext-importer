#!/usr/bin/env python2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import re

import ckanapi


def create_package(api, name_prefix, **kwargs):
    '''
    Create a new package with a temporary name.
    '''
    i = 0
    while True:
        name = '{}{}'.format(name_prefix, i)
        try:
            return api.action.package_create(name=name, **kwargs)
        except ckanapi.ValidationError as e:
            if 'name' in e.error_dict:
                # Duplicate name
                i += 1
                continue
            raise


def find_pkgs_by_extras(api, extras):
    '''
    Find CKAN packages with certain "extra" fields.

    ``extras`` is a dict that maps extra field names to search values.
    The search values are passed literally to Solr, so make sure to
    escape Solr special symbols unless their special meaning is
    intended (see ``solr_escape``).

    Returns a list of package dicts.
    '''
    fq = ' AND '.join('extras_{}:"{}"'.format(*item)
                      for item in extras.items())
    # FIXME: Support for paging
    return api.action.package_search(fq=fq, rows=1000)['results']


_SOLR_ESCAPE_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[/\]^"~*?:])')

def solr_escape(s):
    '''
    Escape strings for Solr queries.
    '''
    return _SOLR_ESCAPE_RE.sub(r'\\\g<char>', s)



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
