# coding: utf-8
from __future__ import absolute_import, division, print_function
import re
import sys
import datetime

import tornado.escape
from tornado.escape import utf8
from tornado.test.util import unittest, gzip_compress, random_bytes
from tornado.util import (
    raise_exc_info, Configurable, exec_in, ArgReplacer,
    timedelta_to_seconds, import_object, re_unescape, is_finalizing, PY3,
    GzipDecompressor,
)
import zlib

if PY3:
    from io import StringIO
else:
    from cStringIO import StringIO


class GzipDecompressorTest(unittest.TestCase):
    def members(self):
        return [
            # Incompressible, so that members span many reads.
            random_bytes(5000),
            b"",
            b"hello " * 2000,
            b"x",
        ]

    def decompress(self, data, read_size=1024, max_length=1024):
        # Drive the decompressor the way _GzipMessageDelegate does.
        decompressor = GzipDecompressor()
        result = bytearray()
        for i in range(0, len(data), read_size):
            chunk = data[i:i + read_size]
            while chunk:
                output = decompressor.decompress(chunk, max_length)
                self.assertLessEqual(len(output), max_length)
                tail = decompressor.unconsumed_tail
                self.assertTrue(output or len(tail) < len(chunk), "no progress")
                chunk = tail
                result += output
        self.assertEqual(decompressor.flush(), b"")
        return bytes(result)

    def test_members(self):
        members = self.members()
        streams = [gzip_compress(m) for m in members]
        cases = [
            (streams[0], members[0]),
            (streams[2], members[2]),
            (b"".join(streams), b"".join(members)),
            (b"".join(streams[1:]), b"".join(members[1:])),
        ]
        for data, expected in cases:
            for read_size in [1, 7, 100, 4096, len(data)]:
                for max_length in [1, 100, 5000, 65536]:
                    self.assertEqual(
                        self.decompress(data, read_size, max_length), expected,
                        (len(data), read_size, max_length))

    def test_member_ends_at_max_length(self):
        # When the output of a member ends exactly at max_length, the next
        # member must still be decompressed, even though the call that
        # fills max_length may leave nothing more to read.
        first = random_bytes(1000)
        data = gzip_compress(first) + gzip_compress(b"second")
        for max_length in [1000, 500, 100]:
            for read_size in [len(data), 100]:
                self.assertEqual(self.decompress(data, read_size, max_length),
                                 first + b"second", (max_length, read_size))

    def test_member_ends_at_read_boundary(self):
        # Python 2's decompressobj has no eof attribute, so a member that
        # ends exactly at the end of one read is only seen to have ended
        # when the next read arrives.
        first = gzip_compress(b"first")
        data = first + gzip_compress(b"second")
        self.assertEqual(self.decompress(data, len(first)), b"firstsecond")

    def test_empty(self):
        # A stream with no members, e.g. the body of a response to a HEAD request.
        decompressor = GzipDecompressor()
        self.assertEqual(decompressor.flush(), b"")

    def test_trailing_data(self):
        data = gzip_compress(b"hello")
        for trailer in [b"\0", b"\0" * 16, b"\r\n", b"garbage", b"\x1f\x00"]:
            for read_size in [1, 1024]:
                with self.assertRaises(zlib.error):
                    self.decompress(data + trailer, read_size)

    def test_trailing_data_after_max_length(self):
        # The reviewer's case: a member that ends in a call following one cut
        # short by max_length leaves the trailing bytes in unconsumed_tail as
        # well as unused_data (a CPython bug). Looping on unconsumed_tail
        # used to spin forever instead of reporting the trailing data.
        data = gzip_compress(random_bytes(70000) + b"x" * 200000)
        with self.assertRaises(zlib.error):
            self.decompress(data + b"TRAILING-GARBAGE", len(data) + 16, 65536)

    def test_truncated(self):
        members = self.members()
        first = gzip_compress(members[0])
        second = gzip_compress(members[2])
        for data in [
            first[:-1],
            first[:len(first) // 2],
            first + second[:1],
            first + second[:-1],
        ]:
            with self.assertRaises(zlib.error):
                self.decompress(data)

    def test_not_gzip(self):
        for data in [b"<html>", zlib.compress(b"hello")]:
            with self.assertRaises(zlib.error):
                self.decompress(data)

    def test_flush_with_unconsumed_tail(self):
        # A decompression bomb that the caller stops feeding back in after
        # the first max_length bytes. flush must not decompress the rest.
        compressor = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        block = b"\0" * (1024 * 1024)
        bomb = b"".join(compressor.compress(block) for _ in range(16))
        bomb += compressor.flush()
        decompressor = GzipDecompressor()
        self.assertEqual(len(decompressor.decompress(bomb, 1024)), 1024)
        self.assertTrue(decompressor.unconsumed_tail)
        with self.assertRaises(ValueError):
            decompressor.flush()

    def test_no_max_length(self):
        # Without max_length, all the members are decompressed in one call.
        # (Upstream also deprecates this; that warning is not backported.)
        data = gzip_compress(b"hello") + gzip_compress(b" world")
        decompressor = GzipDecompressor()
        self.assertEqual(decompressor.decompress(data), b"hello world")
        self.assertEqual(decompressor.flush(), b"")


class RaiseExcInfoTest(unittest.TestCase):
    def test_two_arg_exception(self):
        # This test would fail on python 3 if raise_exc_info were simply
        # a three-argument raise statement, because TwoArgException
        # doesn't have a "copy constructor"
        class TwoArgException(Exception):
            def __init__(self, a, b):
                super(TwoArgException, self).__init__()
                self.a, self.b = a, b

        try:
            raise TwoArgException(1, 2)
        except TwoArgException:
            exc_info = sys.exc_info()
        try:
            raise_exc_info(exc_info)
            self.fail("didn't get expected exception")
        except TwoArgException as e:
            self.assertIs(e, exc_info[1])


class TestConfigurable(Configurable):
    @classmethod
    def configurable_base(cls):
        return TestConfigurable

    @classmethod
    def configurable_default(cls):
        return TestConfig1


class TestConfig1(TestConfigurable):
    def initialize(self, pos_arg=None, a=None):
        self.a = a
        self.pos_arg = pos_arg


class TestConfig2(TestConfigurable):
    def initialize(self, pos_arg=None, b=None):
        self.b = b
        self.pos_arg = pos_arg


class TestConfig3(TestConfigurable):
    # TestConfig3 is a configuration option that is itself configurable.
    @classmethod
    def configurable_base(cls):
        return TestConfig3

    @classmethod
    def configurable_default(cls):
        return TestConfig3A


class TestConfig3A(TestConfig3):
    def initialize(self, a=None):
        self.a = a


class TestConfig3B(TestConfig3):
    def initialize(self, b=None):
        self.b = b


class ConfigurableTest(unittest.TestCase):
    def setUp(self):
        self.saved = TestConfigurable._save_configuration()
        self.saved3 = TestConfig3._save_configuration()

    def tearDown(self):
        TestConfigurable._restore_configuration(self.saved)
        TestConfig3._restore_configuration(self.saved3)

    def checkSubclasses(self):
        # no matter how the class is configured, it should always be
        # possible to instantiate the subclasses directly
        self.assertIsInstance(TestConfig1(), TestConfig1)
        self.assertIsInstance(TestConfig2(), TestConfig2)

        obj = TestConfig1(a=1)
        self.assertEqual(obj.a, 1)
        obj = TestConfig2(b=2)
        self.assertEqual(obj.b, 2)

    def test_default(self):
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig1)
        self.assertIs(obj.a, None)

        obj = TestConfigurable(a=1)
        self.assertIsInstance(obj, TestConfig1)
        self.assertEqual(obj.a, 1)

        self.checkSubclasses()

    def test_config_class(self):
        TestConfigurable.configure(TestConfig2)
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig2)
        self.assertIs(obj.b, None)

        obj = TestConfigurable(b=2)
        self.assertIsInstance(obj, TestConfig2)
        self.assertEqual(obj.b, 2)

        self.checkSubclasses()

    def test_config_args(self):
        TestConfigurable.configure(None, a=3)
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig1)
        self.assertEqual(obj.a, 3)

        obj = TestConfigurable(42, a=4)
        self.assertIsInstance(obj, TestConfig1)
        self.assertEqual(obj.a, 4)
        self.assertEqual(obj.pos_arg, 42)

        self.checkSubclasses()
        # args bound in configure don't apply when using the subclass directly
        obj = TestConfig1()
        self.assertIs(obj.a, None)

    def test_config_class_args(self):
        TestConfigurable.configure(TestConfig2, b=5)
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig2)
        self.assertEqual(obj.b, 5)

        obj = TestConfigurable(42, b=6)
        self.assertIsInstance(obj, TestConfig2)
        self.assertEqual(obj.b, 6)
        self.assertEqual(obj.pos_arg, 42)

        self.checkSubclasses()
        # args bound in configure don't apply when using the subclass directly
        obj = TestConfig2()
        self.assertIs(obj.b, None)

    def test_config_multi_level(self):
        TestConfigurable.configure(TestConfig3, a=1)
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig3A)
        self.assertEqual(obj.a, 1)

        TestConfigurable.configure(TestConfig3)
        TestConfig3.configure(TestConfig3B, b=2)
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig3B)
        self.assertEqual(obj.b, 2)

    def test_config_inner_level(self):
        # The inner level can be used even when the outer level
        # doesn't point to it.
        obj = TestConfig3()
        self.assertIsInstance(obj, TestConfig3A)

        TestConfig3.configure(TestConfig3B)
        obj = TestConfig3()
        self.assertIsInstance(obj, TestConfig3B)

        # Configuring the base doesn't configure the inner.
        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig1)
        TestConfigurable.configure(TestConfig2)

        obj = TestConfigurable()
        self.assertIsInstance(obj, TestConfig2)

        obj = TestConfig3()
        self.assertIsInstance(obj, TestConfig3B)


class UnicodeLiteralTest(unittest.TestCase):
    def test_unicode_escapes(self):
        self.assertEqual(utf8(u'\u00e9'), b'\xc3\xa9')


class ExecInTest(unittest.TestCase):
    # This test is python 2 only because there are no new future imports
    # defined in python 3 yet.
    @unittest.skipIf(sys.version_info >= print_function.getMandatoryRelease(),
                     'no testable future imports')
    def test_no_inherit_future(self):
        # This file has from __future__ import print_function...
        f = StringIO()
        print('hello', file=f)
        # ...but the template doesn't
        exec_in('print >> f, "world"', dict(f=f))
        self.assertEqual(f.getvalue(), 'hello\nworld\n')


class ArgReplacerTest(unittest.TestCase):
    def setUp(self):
        def function(x, y, callback=None, z=None):
            pass
        self.replacer = ArgReplacer(function, 'callback')

    def test_omitted(self):
        args = (1, 2)
        kwargs = dict()
        self.assertIs(self.replacer.get_old_value(args, kwargs), None)
        self.assertEqual(self.replacer.replace('new', args, kwargs),
                         (None, (1, 2), dict(callback='new')))

    def test_position(self):
        args = (1, 2, 'old', 3)
        kwargs = dict()
        self.assertEqual(self.replacer.get_old_value(args, kwargs), 'old')
        self.assertEqual(self.replacer.replace('new', args, kwargs),
                         ('old', [1, 2, 'new', 3], dict()))

    def test_keyword(self):
        args = (1,)
        kwargs = dict(y=2, callback='old', z=3)
        self.assertEqual(self.replacer.get_old_value(args, kwargs), 'old')
        self.assertEqual(self.replacer.replace('new', args, kwargs),
                         ('old', (1,), dict(y=2, callback='new', z=3)))


class TimedeltaToSecondsTest(unittest.TestCase):
    def test_timedelta_to_seconds(self):
        time_delta = datetime.timedelta(hours=1)
        self.assertEqual(timedelta_to_seconds(time_delta), 3600.0)


class ImportObjectTest(unittest.TestCase):
    def test_import_member(self):
        self.assertIs(import_object('tornado.escape.utf8'), utf8)

    def test_import_member_unicode(self):
        self.assertIs(import_object(u'tornado.escape.utf8'), utf8)

    def test_import_module(self):
        self.assertIs(import_object('tornado.escape'), tornado.escape)

    def test_import_module_unicode(self):
        # The internal implementation of __import__ differs depending on
        # whether the thing being imported is a module or not.
        # This variant requires a byte string in python 2.
        self.assertIs(import_object(u'tornado.escape'), tornado.escape)


class ReUnescapeTest(unittest.TestCase):
    def test_re_unescape(self):
        test_strings = (
            '/favicon.ico',
            'index.html',
            'Hello, World!',
            '!$@#%;',
        )
        for string in test_strings:
            self.assertEqual(string, re_unescape(re.escape(string)))

    def test_re_unescape_raises_error_on_invalid_input(self):
        with self.assertRaises(ValueError):
            re_unescape('\\d')
        with self.assertRaises(ValueError):
            re_unescape('\\b')
        with self.assertRaises(ValueError):
            re_unescape('\\Z')


class IsFinalizingTest(unittest.TestCase):
    def test_basic(self):
        self.assertFalse(is_finalizing())
