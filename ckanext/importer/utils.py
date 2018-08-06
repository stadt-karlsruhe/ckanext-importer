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

import collections
import logging

import ckanapi


log = logging.getLogger(__name__)


class context_manager_method(object):
    '''
    Decorator for methods that provide a context manager.

    This decorator allows you to provide context managers from methods
    of a class.

    Usage::

        class Foo:
            def say(self, x):
                print(x)

            @context_manager_method
            class greetings(object):
                """
                A context manager that provides greetings.
                """
                def __init__(self, name):
                    self.name = name

                def __enter__(self):
                    # You can access the instance of the outer class
                    # (``Foo``) using the ``_outer`` attribute.
                    self._outer.say('Hello, {}'.format(self.name))

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self._outer.say('Good bye, {}'.format(self.name))


        foo = Foo()
        with foo.greetings('Sarah'):
            print('Inside the context manager')
    '''
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
                return '<context manager method {}.{} of {}>'.format(
                    obj.__class__.__name__,
                    self._cls.__name__,
                    obj,
                )

        return NestedCM()


class DictWrapper(collections.MutableMapping):
    '''
    Wrapper for an existing dict.

    Helper class for providing a customized ``dict``-interface against
    an existing dict. Subclasses can override that part of the interface
    that they're interested in.
    '''
    def __init__(self, d):
        '''
        Constructor.

        ``d`` is an existing ``dict``. The resulting ``DictWrapper``
        instance will delegate all access to ``d``.
        '''
        self._dict = d

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __delitem__(self, key):
        del self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, key):
        return key in self._dict
