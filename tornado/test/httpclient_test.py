# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import base64
import binascii
from contextlib import closing
import copy
import sys
import threading
import datetime
from io import BytesIO
import time
import unicodedata
import zlib

from tornado.escape import utf8, native_str, json_encode, json_decode
from tornado import gen
from tornado.httpclient import HTTPRequest, HTTPResponse, _RequestProxy, HTTPError, HTTPClient
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado.log import gen_log
from tornado import netutil
from tornado.stack_context import ExceptionStackContext, NullContext
from tornado.testing import AsyncHTTPTestCase, bind_unused_port, gen_test, ExpectLog
from tornado.test.util import unittest, skipOnTravis, ignore_deprecation
from tornado.web import Application, RequestHandler, url
from tornado.httputil import format_timestamp, HTTPHeaders


class HelloWorldHandler(RequestHandler):
    def get(self):
        name = self.get_argument("name", "world")
        self.set_header("Content-Type", "text/plain")
        self.finish("Hello %s!" % name)


class PostHandler(RequestHandler):
    def post(self):
        self.finish("Post arg1: %s, arg2: %s" % (
            self.get_argument("arg1"), self.get_argument("arg2")))


class PutHandler(RequestHandler):
    def put(self):
        self.write("Put body: ")
        self.write(self.request.body)


class RedirectHandler(RequestHandler):
    def prepare(self):
        self.write('redirects can have bodies too')
        self.redirect(self.get_argument("url"),
                      status=int(self.get_argument("status", "302")))


class ChunkHandler(RequestHandler):
    @gen.coroutine
    def get(self):
        self.write("asdf")
        self.flush()
        # Wait a bit to ensure the chunks are sent and received separately.
        yield gen.sleep(0.01)
        self.write("qwer")


class AuthHandler(RequestHandler):
    def get(self):
        self.finish(self.request.headers["Authorization"])


class CountdownHandler(RequestHandler):
    def get(self, count):
        count = int(count)
        if count > 0:
            self.redirect(self.reverse_url("countdown", count - 1))
        else:
            self.write("Zero")


class EchoPostHandler(RequestHandler):
    def post(self):
        self.write(self.request.body)


class UserAgentHandler(RequestHandler):
    def get(self):
        self.write(self.request.headers.get('User-Agent', 'User agent not set'))


class ContentLength304Handler(RequestHandler):
    def get(self):
        self.set_status(304)
        self.set_header('Content-Length', 42)

    def _clear_headers_for_304(self):
        # Tornado strips content-length from 304 responses, but here we
        # want to simulate servers that include the headers anyway.
        pass


class PatchHandler(RequestHandler):

    def patch(self):
        "Return the request payload - so we can check it is being kept"
        self.write(self.request.body)


class AllMethodsHandler(RequestHandler):
    SUPPORTED_METHODS = RequestHandler.SUPPORTED_METHODS + ('OTHER',)

    def method(self):
        self.write(self.request.method)

    get = post = put = delete = options = patch = other = method


class SetHeaderHandler(RequestHandler):
    def get(self):
        # Use get_arguments for keys to get strings, but
        # request.arguments for values to get bytes.
        for k, v in zip(self.get_arguments('k'),
                        self.request.arguments['v']):
            self.set_header(k, v)


class EchoHeadersHandler(RequestHandler):
    def get(self):
        self.write(json_encode(dict(self.request.headers.get_all())))


# Decompressed size of the response served by `GzipBombHandler`. Kept below
# every implementation's default ``max_body_size`` of 100MB so that a
# streaming_callback can retrieve the whole thing; raising it past that limit
# would turn this into a test of the limit instead (see
# `HTTPClientMaxBodySizeTestMixin`). Upstream uses 64MB; 16MB keeps the
# Python 2 run short and is still many times the 1MB streaming buffer.
GZIP_BOMB_SIZE = 16 * 1024 * 1024

_gzip_bombs = {}


def gzip_bomb(size=GZIP_BOMB_SIZE):
    """Returns a small gzip stream that expands to ``size`` bytes.

    Compressed incrementally so that building it does not itself allocate
    the decompressed size.
    """
    if size not in _gzip_bombs:
        compressor = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        block = b"\0" * (1024 * 1024)
        pieces = [compressor.compress(block) for _ in range(size // len(block))]
        pieces.append(compressor.flush())
        _gzip_bombs[size] = b"".join(pieces)
    return _gzip_bombs[size]


_large_body = []


def large_body():
    """Returns a body big enough to be delivered in several chunks.

    The contents are non-repeating so that a chunk which was dropped,
    duplicated or delivered out of order can be detected.
    """
    if not _large_body:
        _large_body.append(b"".join(utf8("%09d\n" % i) for i in range(100000)))
    return _large_body[0]


class GzipBombHandler(RequestHandler):
    """Sends a small response that decompresses to a very large one."""

    def get(self):
        size = int(self.get_argument("size", str(GZIP_BOMB_SIZE)))
        # Set Content-Encoding manually to avoid automatic gzip encoding.
        self.set_header("Content-Encoding", "gzip")
        self.write(gzip_bomb(size))


class LargeBodyHandler(RequestHandler):
    def get(self):
        # An incompressible content type, so that the server's gzip
        # transform leaves the body alone and this exercises the plain
        # streaming path.
        self.set_header("Content-Type", "application/octet-stream")
        self.write(large_body())

# These tests end up getting run redundantly: once here with the default
# HTTPClient implementation, and then again in each implementation's own
# test suite.


class HTTPClientCommonTestCase(AsyncHTTPTestCase):
    def get_app(self):
        return Application([
            url("/hello", HelloWorldHandler),
            url("/post", PostHandler),
            url("/put", PutHandler),
            url("/redirect", RedirectHandler),
            url("/chunk", ChunkHandler),
            url("/auth", AuthHandler),
            url("/countdown/([0-9]+)", CountdownHandler, name="countdown"),
            url("/echopost", EchoPostHandler),
            url("/user_agent", UserAgentHandler),
            url("/304_with_content_length", ContentLength304Handler),
            url("/all_methods", AllMethodsHandler),
            url('/patch', PatchHandler),
            url('/set_header', SetHeaderHandler),
            url("/echo_headers", EchoHeadersHandler),
            url("/gzip_bomb", GzipBombHandler),
            url("/large_body", LargeBodyHandler),
        ], gzip=True)

    def setUp(self):
        super(HTTPClientCommonTestCase, self).setUp()

        # Add a second port (serving the same app) to the HTTP server, so we can test the effects
        # of redirects that span different origins.
        sock, port = bind_unused_port()
        self.http_server.add_socket(sock)
        self.__port2 = port

    def get_url2(self, path):
        return "%s://127.0.0.1:%s%s" % (self.get_protocol(), self.__port2, path)

    def test_patch_receives_payload(self):
        body = b"some patch data"
        response = self.fetch("/patch", method='PATCH', body=body)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, body)

    @skipOnTravis
    def test_hello_world(self):
        response = self.fetch("/hello")
        self.assertEqual(response.code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/plain")
        self.assertEqual(response.body, b"Hello world!")
        self.assertEqual(int(response.request_time), 0)

        response = self.fetch("/hello?name=Ben")
        self.assertEqual(response.body, b"Hello Ben!")

    def test_streaming_callback(self):
        # streaming_callback is also tested in test_chunked
        chunks = []
        response = self.fetch("/hello",
                              streaming_callback=chunks.append)
        # with streaming_callback, data goes to the callback and not response.body
        self.assertEqual(chunks, [b"Hello world!"])
        self.assertFalse(response.body)

    def test_streaming_callback_large_body(self):
        # A body large enough to arrive in several chunks must reach the
        # callback exactly once each, in order.
        chunks = []
        response = self.fetch("/large_body", streaming_callback=chunks.append)
        self.assertEqual(response.code, 200)
        self.assertEqual(b"".join(chunks), large_body())
        self.assertFalse(response.body)

    def test_streaming_decompression_bomb(self):
        # A malicious server can turn a small compressed response into a
        # large decompressed one. With a streaming_callback it must be
        # possible to consume the whole thing. (Upstream also bounds the
        # peak memory with tracemalloc, which Python 2 lacks; that the
        # client holds no more than a bounded amount at a time is covered
        # by CurlStreamingBufferTest.)
        received = [0]

        def streaming_callback(chunk):
            received[0] += len(chunk)

        response = self.fetch("/gzip_bomb", streaming_callback=streaming_callback)
        self.assertEqual(response.code, 200)
        self.assertEqual(received[0], GZIP_BOMB_SIZE)
        self.assertFalse(response.body)

    def test_post(self):
        response = self.fetch("/post", method="POST",
                              body="arg1=foo&arg2=bar")
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b"Post arg1: foo, arg2: bar")

    def test_chunked(self):
        response = self.fetch("/chunk")
        self.assertEqual(response.body, b"asdfqwer")

        chunks = []
        response = self.fetch("/chunk",
                              streaming_callback=chunks.append)
        self.assertEqual(chunks, [b"asdf", b"qwer"])
        self.assertFalse(response.body)

    def test_chunked_close(self):
        # test case in which chunks spread read-callback processing
        # over several ioloop iterations, but the connection is already closed.
        sock, port = bind_unused_port()
        with closing(sock):
            @gen.coroutine
            def accept_callback(conn, address):
                # fake an HTTP server using chunked encoding where the final chunks
                # and connection close all happen at once
                stream = IOStream(conn)
                request_data = yield stream.read_until(b"\r\n\r\n")
                if b"HTTP/1." not in request_data:
                    self.skipTest("requires HTTP/1.x")
                yield stream.write(b"""\
HTTP/1.1 200 OK
Transfer-Encoding: chunked

1
1
1
2
0

""".replace(b"\n", b"\r\n"))
                stream.close()
            netutil.add_accept_handler(sock, accept_callback)
            resp = self.fetch("http://127.0.0.1:%d/" % port)
            resp.rethrow()
            self.assertEqual(resp.body, b"12")
            self.io_loop.remove_handler(sock.fileno())

    def test_streaming_stack_context(self):
        chunks = []
        exc_info = []

        def error_handler(typ, value, tb):
            exc_info.append((typ, value, tb))
            return True

        def streaming_cb(chunk):
            chunks.append(chunk)
            if chunk == b'qwer':
                1 / 0

        with ignore_deprecation():
            with ExceptionStackContext(error_handler):
                self.fetch('/chunk', streaming_callback=streaming_cb)

        self.assertEqual(chunks, [b'asdf', b'qwer'])
        self.assertEqual(1, len(exc_info))
        self.assertIs(exc_info[0][0], ZeroDivisionError)

    def test_basic_auth(self):
        # This test data appears in section 2 of RFC 7617.
        self.assertEqual(self.fetch("/auth", auth_username="Aladdin",
                                    auth_password="open sesame").body,
                         b"Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==")

    def test_basic_auth_explicit_mode(self):
        self.assertEqual(self.fetch("/auth", auth_username="Aladdin",
                                    auth_password="open sesame",
                                    auth_mode="basic").body,
                         b"Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==")

    def test_basic_auth_unicode(self):
        # This test data appears in section 2.1 of RFC 7617.
        self.assertEqual(self.fetch("/auth", auth_username="test",
                                    auth_password="123£").body,
                         b"Basic dGVzdDoxMjPCow==")

        # The standard mandates NFC. Give it a decomposed username
        # and ensure it is normalized to composed form.
        username = unicodedata.normalize("NFD", u"josé")
        self.assertEqual(self.fetch("/auth",
                                    auth_username=username,
                                    auth_password="səcrət").body,
                         b"Basic am9zw6k6c8mZY3LJmXQ=")

    def test_unsupported_auth_mode(self):
        # curl and simple clients handle errors a bit differently; the
        # important thing is that they don't fall back to basic auth
        # on an unknown mode.
        with ExpectLog(gen_log, "uncaught exception", required=False):
            with self.assertRaises((ValueError, HTTPError)):
                self.fetch("/auth", auth_username="Aladdin",
                           auth_password="open sesame",
                           auth_mode="asdf",
                           raise_error=True)

    def test_follow_redirect(self):
        response = self.fetch("/countdown/2", follow_redirects=False)
        self.assertEqual(302, response.code)
        self.assertTrue(response.headers["Location"].endswith("/countdown/1"))

        response = self.fetch("/countdown/2")
        self.assertEqual(200, response.code)
        self.assertTrue(response.effective_url.endswith("/countdown/0"))
        self.assertEqual(b"Zero", response.body)

    def test_credentials_in_url(self):
        url = self.get_url("/auth").replace("http://", "http://me:secret@")
        response = self.fetch(url)
        self.assertEqual(b"Basic " + base64.b64encode(b"me:secret"),
                         response.body)

    def test_body_encoding(self):
        unicode_body = u"\xe9"
        byte_body = binascii.a2b_hex(b"e9")

        # unicode string in body gets converted to utf8
        response = self.fetch("/echopost", method="POST", body=unicode_body,
                              headers={"Content-Type": "application/blah"})
        self.assertEqual(response.headers["Content-Length"], "2")
        self.assertEqual(response.body, utf8(unicode_body))

        # byte strings pass through directly
        response = self.fetch("/echopost", method="POST",
                              body=byte_body,
                              headers={"Content-Type": "application/blah"})
        self.assertEqual(response.headers["Content-Length"], "1")
        self.assertEqual(response.body, byte_body)

        # Mixing unicode in headers and byte string bodies shouldn't
        # break anything
        response = self.fetch("/echopost", method="POST", body=byte_body,
                              headers={"Content-Type": "application/blah"},
                              user_agent=u"foo")
        self.assertEqual(response.headers["Content-Length"], "1")
        self.assertEqual(response.body, byte_body)

    def test_types(self):
        response = self.fetch("/hello")
        self.assertEqual(type(response.body), bytes)
        self.assertEqual(type(response.headers["Content-Type"]), str)
        self.assertEqual(type(response.code), int)
        self.assertEqual(type(response.effective_url), str)

    def test_header_callback(self):
        first_line = []
        headers = {}
        chunks = []

        def header_callback(header_line):
            if header_line.startswith('HTTP/1.1 101'):
                # Upgrading to HTTP/2
                pass
            elif header_line.startswith('HTTP/'):
                first_line.append(header_line)
            elif header_line != '\r\n':
                k, v = header_line.split(':', 1)
                headers[k.lower()] = v.strip()

        def streaming_callback(chunk):
            # All header callbacks are run before any streaming callbacks,
            # so the header data is available to process the data as it
            # comes in.
            self.assertEqual(headers['content-type'], 'text/html; charset=UTF-8')
            chunks.append(chunk)

        self.fetch('/chunk', header_callback=header_callback,
                   streaming_callback=streaming_callback)
        self.assertEqual(len(first_line), 1, first_line)
        self.assertRegexpMatches(first_line[0], 'HTTP/[0-9]\\.[0-9] 200.*\r\n')
        self.assertEqual(chunks, [b'asdf', b'qwer'])

    def test_header_callback_stack_context(self):
        exc_info = []

        def error_handler(typ, value, tb):
            exc_info.append((typ, value, tb))
            return True

        def header_callback(header_line):
            if header_line.lower().startswith('content-type:'):
                1 / 0

        with ignore_deprecation():
            with ExceptionStackContext(error_handler):
                self.fetch('/chunk', header_callback=header_callback)
        self.assertEqual(len(exc_info), 1)
        self.assertIs(exc_info[0][0], ZeroDivisionError)

    @gen_test
    def test_configure_defaults(self):
        defaults = dict(user_agent='TestDefaultUserAgent', allow_ipv6=False)
        # Construct a new instance of the configured client class
        client = self.http_client.__class__(force_instance=True,
                                            defaults=defaults)
        try:
            response = yield client.fetch(self.get_url('/user_agent'))
            self.assertEqual(response.body, b'TestDefaultUserAgent')
        finally:
            client.close()

    def test_header_types(self):
        # Header values may be passed as character or utf8 byte strings,
        # in a plain dictionary or an HTTPHeaders object.
        # Keys must always be the native str type.
        # All combinations should have the same results on the wire.
        for value in [u"MyUserAgent", b"MyUserAgent"]:
            for container in [dict, HTTPHeaders]:
                headers = container()
                headers['User-Agent'] = value
                resp = self.fetch('/user_agent', headers=headers)
                self.assertEqual(
                    resp.body, b"MyUserAgent",
                    "response=%r, value=%r, container=%r" %
                    (resp.body, value, container))

    def test_multi_line_headers(self):
        # Multi-line http headers are rare but rfc-allowed
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
        sock, port = bind_unused_port()
        with closing(sock):
            @gen.coroutine
            def accept_callback(conn, address):
                stream = IOStream(conn)
                request_data = yield stream.read_until(b"\r\n\r\n")
                if b"HTTP/1." not in request_data:
                    self.skipTest("requires HTTP/1.x")
                yield stream.write(b"""\
HTTP/1.1 200 OK
X-XSS-Protection: 1;
\tmode=block

""".replace(b"\n", b"\r\n"))
                stream.close()

            netutil.add_accept_handler(sock, accept_callback)
            resp = self.fetch("http://127.0.0.1:%d/" % port)
            resp.rethrow()
            self.assertEqual(resp.headers['X-XSS-Protection'], "1; mode=block")
            self.io_loop.remove_handler(sock.fileno())

    def test_304_with_content_length(self):
        # According to the spec 304 responses SHOULD NOT include
        # Content-Length or other entity headers, but some servers do it
        # anyway.
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.3.5
        response = self.fetch('/304_with_content_length')
        self.assertEqual(response.code, 304)
        self.assertEqual(response.headers['Content-Length'], '42')

    def test_final_callback_stack_context(self):
        # The final callback should be run outside of the httpclient's
        # stack_context.  We want to ensure that there is not stack_context
        # between the user's callback and the IOLoop, so monkey-patch
        # IOLoop.handle_callback_exception and disable the test harness's
        # context with a NullContext.
        # Note that this does not apply to secondary callbacks (header
        # and streaming_callback), as errors there must be seen as errors
        # by the http client so it can clean up the connection.
        exc_info = []

        def handle_callback_exception(callback):
            exc_info.append(sys.exc_info())
            self.stop()
        self.io_loop.handle_callback_exception = handle_callback_exception
        with NullContext():
            with ignore_deprecation():
                self.http_client.fetch(self.get_url('/hello'),
                                       lambda response: 1 / 0)
        self.wait()
        self.assertEqual(exc_info[0][0], ZeroDivisionError)

    @gen_test
    def test_future_interface(self):
        response = yield self.http_client.fetch(self.get_url('/hello'))
        self.assertEqual(response.body, b'Hello world!')

    @gen_test
    def test_future_http_error(self):
        with self.assertRaises(HTTPError) as context:
            yield self.http_client.fetch(self.get_url('/notfound'))
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.response.code, 404)

    @gen_test
    def test_future_http_error_no_raise(self):
        response = yield self.http_client.fetch(self.get_url('/notfound'), raise_error=False)
        self.assertEqual(response.code, 404)

    @gen_test
    def test_reuse_request_from_response(self):
        # The response.request attribute should be an HTTPRequest, not
        # a _RequestProxy.
        # This test uses self.http_client.fetch because self.fetch calls
        # self.get_url on the input unconditionally.
        url = self.get_url('/hello')
        response = yield self.http_client.fetch(url)
        self.assertEqual(response.request.url, url)
        self.assertTrue(isinstance(response.request, HTTPRequest))
        response2 = yield self.http_client.fetch(response.request)
        self.assertEqual(response2.body, b'Hello world!')

    def test_all_methods(self):
        for method in ['GET', 'DELETE', 'OPTIONS']:
            response = self.fetch('/all_methods', method=method)
            self.assertEqual(response.body, utf8(method))
        for method in ['POST', 'PUT', 'PATCH']:
            response = self.fetch('/all_methods', method=method, body=b'')
            self.assertEqual(response.body, utf8(method))
        response = self.fetch('/all_methods', method='HEAD')
        self.assertEqual(response.body, b'')
        response = self.fetch('/all_methods', method='OTHER',
                              allow_nonstandard_methods=True)
        self.assertEqual(response.body, b'OTHER')

    def test_body_sanity_checks(self):
        # These methods require a body.
        for method in ('POST', 'PUT', 'PATCH'):
            with self.assertRaises(ValueError) as context:
                self.fetch('/all_methods', method=method, raise_error=True)
            self.assertIn('must not be None', str(context.exception))

            resp = self.fetch('/all_methods', method=method,
                              allow_nonstandard_methods=True)
            self.assertEqual(resp.code, 200)

        # These methods don't allow a body.
        for method in ('GET', 'DELETE', 'OPTIONS'):
            with self.assertRaises(ValueError) as context:
                self.fetch('/all_methods', method=method, body=b'asdf', raise_error=True)
            self.assertIn('must be None', str(context.exception))

            # In most cases this can be overridden, but curl_httpclient
            # does not allow body with a GET at all.
            if method != 'GET':
                self.fetch('/all_methods', method=method, body=b'asdf',
                           allow_nonstandard_methods=True, raise_error=True)
                self.assertEqual(resp.code, 200)

    # This test causes odd failures with the combination of
    # curl_httpclient (at least with the version of libcurl available
    # on ubuntu 12.04), TwistedIOLoop, and epoll.  For POST (but not PUT),
    # curl decides the response came back too soon and closes the connection
    # to start again.  It does this *before* telling the socket callback to
    # unregister the FD.  Some IOLoop implementations have special kernel
    # integration to discover this immediately.  Tornado's IOLoops
    # ignore errors on remove_handler to accommodate this behavior, but
    # Twisted's reactor does not.  The removeReader call fails and so
    # do all future removeAll calls (which our tests do at cleanup).
    #
    # def test_post_307(self):
    #    response = self.fetch("/redirect?status=307&url=/post",
    #                          method="POST", body=b"arg1=foo&arg2=bar")
    #    self.assertEqual(response.body, b"Post arg1: foo, arg2: bar")

    def test_put_307(self):
        response = self.fetch("/redirect?status=307&url=/put",
                              method="PUT", body=b"hello")
        response.rethrow()
        self.assertEqual(response.body, b"Put body: hello")

    def test_non_ascii_header(self):
        # Non-ascii headers are sent as latin1.
        response = self.fetch("/set_header?k=foo&v=%E9")
        response.rethrow()
        self.assertEqual(response.headers["Foo"], native_str(u"\u00e9"))

    def test_response_times(self):
        # A few simple sanity checks of the response time fields to
        # make sure they're using the right basis (between the
        # wall-time and monotonic clocks).
        start_time = time.time()
        response = self.fetch("/hello")
        response.rethrow()
        self.assertGreaterEqual(response.request_time, 0)
        self.assertLess(response.request_time, 1.0)
        # A very crude check to make sure that start_time is based on
        # wall time and not the monotonic clock.
        self.assertLess(abs(response.start_time - start_time), 1.0)

        for k, v in response.time_info.items():
            self.assertTrue(0 <= v < 1.0, "time_info[%s] out of bounds: %s" % (k, v))

    def test_strip_headers_on_redirect(self):
        # CVE-2026-49853: ensure that headers that should be stripped on
        # cross-origin redirects are stripped, even if the redirect is to a
        # different port on localhost.
        test_cases = [
            ("manual auth header", dict(headers={"Authorization": "secret"}), ""),
            ("credentials in URL", dict(), "me:secret"),
            ("auth parameters", dict(auth_username="me", auth_password="secret"), ""),
            ("manual cookie header", dict(headers={"Cookie": "secret"}), ""),
        ]
        for name, kwargs, url_creds in test_cases:
            for origin, target in (("different", self.get_url2), ("same", self.get_url)):
                msg = "%s, %s origin" % (name, origin)
                url = self.get_url(
                    "/redirect?url=%s&status=302" % target("/echo_headers"))
                if url_creds:
                    url = url.replace("http://", "http://%s@" % url_creds)
                response = self.fetch(url, **kwargs)
                response.rethrow()
                echoed_headers = json_decode(response.body)
                # Confirm that non-auth headers are getting through.
                # (Upstream checks User-Agent, which 5.1.1's simple client
                # does not send by default.)
                self.assertIn("Host", echoed_headers, msg)
                if origin == "different":
                    # Auth headers are stripped, however they were set.
                    self.assertNotIn("Authorization", echoed_headers, msg)
                    self.assertNotIn("Cookie", echoed_headers, msg)
                else:
                    # Auth headers are not stripped when the redirect is
                    # same-origin. Each of our tests uses one of these
                    # headers, but not both.
                    self.assertTrue(
                        "Authorization" in echoed_headers or "Cookie" in echoed_headers,
                        msg)

    def test_redirect_does_not_modify_original_headers(self):
        # The redirected request is a shallow copy of the original, so
        # stripping auth headers for a cross-origin redirect must happen on
        # a copy of the headers, not on the caller's request.
        request = HTTPRequest(
            self.get_url("/redirect?url=%s&status=302" % self.get_url2("/echo_headers")),
            headers={"Authorization": "secret"})
        response = self.io_loop.run_sync(
            lambda: self.http_client.fetch(request, raise_error=False))
        response.rethrow()
        self.assertEqual(request.headers.get("Authorization"), "secret")


class RequestProxyTest(unittest.TestCase):
    def test_request_set(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/',
                                          user_agent='foo'),
                              dict())
        self.assertEqual(proxy.user_agent, 'foo')

    def test_default_set(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/'),
                              dict(network_interface='foo'))
        self.assertEqual(proxy.network_interface, 'foo')

    def test_both_set(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/',
                                          proxy_host='foo'),
                              dict(proxy_host='bar'))
        self.assertEqual(proxy.proxy_host, 'foo')

    def test_neither_set(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/'),
                              dict())
        self.assertIs(proxy.auth_username, None)

    def test_bad_attribute(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/'),
                              dict())
        with self.assertRaises(AttributeError):
            proxy.foo

    def test_defaults_none(self):
        proxy = _RequestProxy(HTTPRequest('http://example.com/'), None)
        self.assertIs(proxy.auth_username, None)


class HTTPResponseTestCase(unittest.TestCase):
    def test_str(self):
        response = HTTPResponse(HTTPRequest('http://example.com'),
                                200, headers={}, buffer=BytesIO())
        s = str(response)
        self.assertTrue(s.startswith('HTTPResponse('))
        self.assertIn('code=200', s)


class SyncHTTPClientTest(unittest.TestCase):
    def setUp(self):
        if IOLoop.configured_class().__name__ == 'TwistedIOLoop':
            # TwistedIOLoop only supports the global reactor, so we can't have
            # separate IOLoops for client and server threads.
            raise unittest.SkipTest(
                'Sync HTTPClient not compatible with TwistedIOLoop')
        self.server_ioloop = IOLoop()

        @gen.coroutine
        def init_server():
            sock, self.port = bind_unused_port()
            app = Application([('/', HelloWorldHandler)])
            self.server = HTTPServer(app)
            self.server.add_socket(sock)
        self.server_ioloop.run_sync(init_server)

        self.server_thread = threading.Thread(target=self.server_ioloop.start)
        self.server_thread.start()

        self.http_client = HTTPClient()

    def tearDown(self):
        def stop_server():
            self.server.stop()
            # Delay the shutdown of the IOLoop by several iterations because
            # the server may still have some cleanup work left when
            # the client finishes with the response (this is noticeable
            # with http/2, which leaves a Future with an unexamined
            # StreamClosedError on the loop).

            @gen.coroutine
            def slow_stop():
                # The number of iterations is difficult to predict. Typically,
                # one is sufficient, although sometimes it needs more.
                for i in range(5):
                    yield
                self.server_ioloop.stop()
            self.server_ioloop.add_callback(slow_stop)
        self.server_ioloop.add_callback(stop_server)
        self.server_thread.join()
        self.http_client.close()
        self.server_ioloop.close(all_fds=True)

    def get_url(self, path):
        return 'http://127.0.0.1:%d%s' % (self.port, path)

    def test_sync_client(self):
        response = self.http_client.fetch(self.get_url('/'))
        self.assertEqual(b'Hello world!', response.body)

    def test_sync_client_error(self):
        # Synchronous HTTPClient raises errors directly; no need for
        # response.rethrow()
        with self.assertRaises(HTTPError) as assertion:
            self.http_client.fetch(self.get_url('/notfound'))
        self.assertEqual(assertion.exception.code, 404)


class HTTPRequestTestCase(unittest.TestCase):
    def test_headers(self):
        request = HTTPRequest('http://example.com', headers={'foo': 'bar'})
        self.assertEqual(request.headers, {'foo': 'bar'})

    def test_headers_setter(self):
        request = HTTPRequest('http://example.com')
        request.headers = {'bar': 'baz'}
        self.assertEqual(request.headers, {'bar': 'baz'})

    def test_null_headers_setter(self):
        request = HTTPRequest('http://example.com')
        request.headers = None
        self.assertEqual(request.headers, {})

    def test_body(self):
        request = HTTPRequest('http://example.com', body='foo')
        self.assertEqual(request.body, utf8('foo'))

    def test_body_setter(self):
        request = HTTPRequest('http://example.com')
        request.body = 'foo'
        self.assertEqual(request.body, utf8('foo'))

    def test_if_modified_since(self):
        http_date = datetime.datetime.utcnow()
        request = HTTPRequest('http://example.com', if_modified_since=http_date)
        self.assertEqual(request.headers,
                         {'If-Modified-Since': format_timestamp(http_date)})


class HTTPErrorTestCase(unittest.TestCase):
    def test_copy(self):
        e = HTTPError(403)
        e2 = copy.copy(e)
        self.assertIsNot(e, e2)
        self.assertEqual(e.code, e2.code)

    def test_plain_error(self):
        e = HTTPError(403)
        self.assertEqual(str(e), "HTTP 403: Forbidden")
        self.assertEqual(repr(e), "HTTP 403: Forbidden")

    def test_error_with_response(self):
        resp = HTTPResponse(HTTPRequest('http://example.com/'), 403)
        with self.assertRaises(HTTPError) as cm:
            resp.rethrow()
        e = cm.exception
        self.assertEqual(str(e), "HTTP 403: Forbidden")
        self.assertEqual(repr(e), "HTTP 403: Forbidden")


# Limit used by `HTTPClientMaxBodySizeTestMixin`. Subclasses configure their
# client with it.
MAX_BODY_SIZE = 1024 * 64


class MaxBodySizeSmallHandler(RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/octet-stream")
        self.write(b"a" * MAX_BODY_SIZE)


class MaxBodySizeLargeHandler(RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/octet-stream")
        self.write(b"a" * (MAX_BODY_SIZE + 1))


class BodyBearingRedirectHandler(RequestHandler):
    """A redirect carrying a body of its own, larger than the limit.

    Implementations differ here: libcurl discards the body of a redirect it
    follows, while simple_httpclient applies ``max_body_size`` to every
    response including the redirect, and so refuses this one before
    following it.
    """

    def prepare(self):
        self.write("x" * (MAX_BODY_SIZE * 2))
        self.redirect("/small")


class HTTPClientMaxBodySizeTestMixin(object):
    """Tests for the limit on a response body held in memory
    (CVE-2026-49855, GHSA-chx6-46f5-w4vp).

    Without a ``streaming_callback`` the whole body is buffered, so a
    response larger than the client is prepared to hold cannot be retrieved
    at all -- it can only be refused. Mixed into an `.AsyncHTTPTestCase`
    whose client is configured with `MAX_BODY_SIZE` (upstream's
    abstract_base_test helper does not exist here).
    """

    # Format of the message the implementation logs to `.gen_log` when it
    # refuses a body, or None if it does not log one. ``{reason}`` is filled
    # in with why the body was refused.
    refusal_log_format = None

    def get_app(self):
        return Application([
            url("/small", MaxBodySizeSmallHandler),
            url("/large", MaxBodySizeLargeHandler),
            url("/bomb", GzipBombHandler),
            url("/body_bearing_redirect", BodyBearingRedirectHandler),
        ])

    def assert_refused(self, path, reason, **fetch_kwargs):
        if self.refusal_log_format is not None:
            expect = ExpectLog(gen_log, self.refusal_log_format.format(reason=reason))
        else:
            expect = _NullContext()
        with expect:
            with self.assertRaises(HTTPError) as cm:
                self.fetch(path, raise_error=True, **fetch_kwargs)
        self.assertEqual(cm.exception.code, 599)

    def assert_streaming_refused(self, path, reason):
        received = [0]

        def streaming_callback(chunk):
            received[0] += len(chunk)

        self.assert_refused(path, reason=reason, streaming_callback=streaming_callback)
        # Nothing past the limit reached the callback.
        self.assertLessEqual(received[0], MAX_BODY_SIZE)

    def test_small_body(self):
        # A body of exactly max_body_size is accepted.
        response = self.fetch("/small")
        response.rethrow()
        self.assertEqual(response.body, b"a" * MAX_BODY_SIZE)

    def test_large_body(self):
        # One byte more is not.
        self.assert_refused("/large", reason="Content-Length too long")

    def test_decompressed_body_size(self):
        # The limit is measured after decompression, so a compressed
        # response cannot expand past it.
        self.assert_refused("/bomb", reason="decompressed body too large")

    def test_streaming_callback_over_limit(self):
        # max_body_size applies to a streaming_callback as well as to a
        # buffered response. It is a safeguard for applications that do not
        # impose a limit of their own on what they do with the chunks
        # (writing them to disk, say), so retrieving a response larger than
        # this means raising the limit deliberately.
        self.assert_streaming_refused("/large", reason="Content-Length too long")

    def test_streaming_callback_decompressed_over_limit(self):
        # As above, measured after decompression.
        self.assert_streaming_refused("/bomb", reason="decompressed body too large")

    def test_reuse_after_refusal(self):
        # Refusing a body must not leave the client unable to make the next
        # request. This matters most for curl_httpclient, which reuses a
        # pool of curl handles.
        self.assert_refused("/large", reason="Content-Length too long")
        response = self.fetch("/small")
        response.rethrow()
        self.assertEqual(response.body, b"a" * MAX_BODY_SIZE)


class _NullContext(object):
    def __enter__(self):
        pass

    def __exit__(self, typ, value, tb):
        pass
