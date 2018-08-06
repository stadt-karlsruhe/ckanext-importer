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

import pytest

from ckanext.importer.utils import context_manager_method, DictWrapper


class TestDictWrapper(object):

    def test_getitem_ok(self):
        d = {'hello': 'world'}
        w = DictWrapper(d)
        assert w['hello'] == 'world'
        d['foo'] = 'bar'
        assert w['foo'] == 'bar'

    def test_getitem_fail(self):
        d = {'hello': 'world'}
        w = DictWrapper(d)
        with pytest.raises(KeyError):
            w['foo']
        del d['hello']
        with pytest.raises(KeyError):
            w['hello']

    def test_setitem(self):
        d = {}
        w = DictWrapper(d)
        w['foo'] = 'bar'
        assert d['foo'] == 'bar'

    def test_delitem_ok(self):
        d = {'hello': 'world'}
        w = DictWrapper(d)
        del w['hello']
        assert 'hello' not in w
        assert 'hello' not in d

    def test_delitem_fail(self):
        d = {}
        w = DictWrapper(d)
        with pytest.raises(KeyError):
            del w['hello']

    def test_iter(self):
        d = {'hello': 'world', 'foo': 'bar'}
        w = DictWrapper(d)
        assert set(w) == {'hello', 'foo'}

    def test_len(self):
        d = {'hello': 'world', 'foo': 'bar'}
        w = DictWrapper(d)
        assert len(w) == 2


class TestContextManagerMethod(object):

    class Foo(object):
        def __init__(self, value):
            self.value = value

        @context_manager_method
        class cm(object):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        def __repr__(self):
            return 'Foo({})'.format(self.value)

    def test_outer(self):
        foo = self.Foo('x')
        with foo.cm() as cm:
            assert cm._outer.value == 'x'

    def test_repr(self):
        foo = self.Foo('x')
        assert repr(foo.cm) == '<context manager method Foo.cm of Foo(x)>'

