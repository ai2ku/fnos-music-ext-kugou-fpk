#!/usr/bin/env python3
"""
fnmusic_ext_kugou.sock 桥接器

飞牛统一网关会把 /app/fnmusic_ext_kugou 前缀的请求转发到 target/fnmusic_ext_kugou.sock。
这个桥接器只做一件事：
  1) 校验前缀
  2) 剥掉 /app/fnmusic_ext_kugou
  3) 原样转发到 /var/run/trim_music.socket
  4) 把响应原样回传
"""

from __future__ import annotations

import logging
import os
import socket
import socketserver
import sys

GATEWAY_PREFIX = "/app/fnmusic_ext_kugou"
GATEWAY_PREFIX_STRICT = "/app/fnmusic_ext_kugou/"
UPSTREAM_SOCK = os.environ.get("FNMUSIC_UPSTREAM_SOCK", "/var/run/trim_music.socket")
LISTEN_SOCK = os.environ.get("FNMUSIC_APP_SOCK", "/var/apps/fnmusic_ext_kugou/target/fnmusic_ext_kugou.sock")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [kugou.sock] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("kugou.sock")


def _read_request_header(sock: socket.socket) -> tuple[str, dict[str, str], str, str]:
    """读取 HTTP 请求首段；body 由后续转发时按 Content-Length 读取。"""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(8192)
        if not chunk:
            raise ConnectionError("client closed before header")
        data += chunk
        if len(data) > 1024 * 1024:
            raise ConnectionError("request header too large")
    head, _, rest = data.partition(b"\r\n\r\n")
    lines = head.decode("latin1").split("\r\n")
    if not lines:
        raise ConnectionError("empty request")
    request_line = lines[0]
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
    return request_line, headers, request_line.split(" ")[1], rest.decode("latin1", errors="ignore")


def _forward_body_with_pre(sock: socket.socket, content_length: int, out: socket.socket, pre: bytes) -> None:
    """先把已收到的 body 前缀发出去，再补读剩下的到 content_length。"""
    sent = 0
    if pre:
        out.sendall(pre)
        sent = len(pre)
    while sent < content_length:
        chunk = sock.recv(min(65536, content_length - sent))
        if not chunk:
            raise ConnectionError("client closed during body")
        out.sendall(chunk)
        sent += len(chunk)


def _forward_body(sock: socket.socket, content_length: int, out: socket.socket) -> None:
    _forward_body_with_pre(sock, content_length, out, b"")


def _forward_response(upstream: socket.socket, out: socket.socket) -> None:
    while True:
        chunk = upstream.recv(65536)
        if not chunk:
            break
        out.sendall(chunk)


def _copy_remaining(client: socket.socket, upstream: socket.socket, rest: str) -> None:
    if rest:
        upstream.sendall(rest.encode("latin1", errors="replace"))
    while True:
        chunk = client.recv(65536)
        if not chunk:
            break
        upstream.sendall(chunk)


def _rewrite_host_headers(headers: dict[str, str]) -> None:
    headers["x-forwarded-proto"] = headers.get("x-forwarded-proto", "http")
    headers["x-forwarded-for"] = headers.get("x-forwarded-for", "127.0.0.1")


def _rewrite_request_target(method: str, path: str, headers: dict[str, str], upstream: socket.socket) -> None:
    _rewrite_host_headers(headers)
    headers["host"] = "localhost"
    # 保留完整前缀，FastAPI 侧按 /app/fnmusic_ext_kugou 路由匹配。
    target_path = path if path != GATEWAY_PREFIX else GATEWAY_PREFIX_STRICT
    req_line = f"{method} {target_path} HTTP/1.1\r\n"
    for k, v in headers.items():
        if k in {"host", "content-length", "content-type", "user-agent", "x-trim-userid", "x-trim-username", "x-trim-isadmin", "x-forwarded-for", "x-forwarded-proto", "accept", "accept-language", "cookie"}:
            req_line += f"{k}: {v}\r\n"
    req_line += "\r\n"
    upstream.sendall(req_line.encode("latin1", errors="replace"))


def _handle(client: socket.socket, client_addr, rest_b: bytes) -> None:
    try:
        request_line, headers, path, rest = _read_request_header(client)
        log.debug("recv %s %s from %s", request_line.split()[0], path, client_addr)
        if not path.startswith(GATEWAY_PREFIX):
            client.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            return
        if path == GATEWAY_PREFIX:
            path = GATEWAY_PREFIX_STRICT
        content_length = headers.get("content-length", "0")
        try:
            clen = int(content_length)
        except ValueError:
            clen = 0

        upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            upstream.connect(UPSTREAM_SOCK)
        except FileNotFoundError:
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 32\r\n\r\nupstream socket not found")
            log.warning("upstream socket missing: %s", UPSTREAM_SOCK)
            return
        except Exception as e:
            client.sendall((f"HTTP/1.1 502 Bad Gateway\r\nContent-Length: {len(str(e))}\r\n\r\n{e}".encode("utf-8")))
            log.warning("upstream connect failed: %s", e)
            return

        try:
            _rewrite_request_target(request_line.split()[0], path, headers, upstream)
            log.debug("-> forwarding %s %s (clen=%d, rest=%d)",
                     request_line.split()[0], path, clen, len(rest))
            _forward_body_with_pre(client, clen, upstream, rest.encode("latin1", errors="replace"))
            log.debug("-> body sent, waiting response")
            _forward_response(upstream, client)
            log.debug("-> response sent, done")
        finally:
            try:
                upstream.close()
            except OSError:
                pass
    except Exception as e:
        log.warning("handler error: %s", e)
        try:
            client.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
        except OSError:
            pass


class ThreadingServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def finish_request(self, request, client_address):
        try:
            _handle(request, client_address, b"")
        finally:
            try:
                request.close()
            except Exception:
                pass


def main() -> int:
    if os.path.exists(LISTEN_SOCK):
        try:
            os.unlink(LISTEN_SOCK)
        except OSError:
            pass
    server = ThreadingServer(LISTEN_SOCK, None)
    try:
        os.chmod(LISTEN_SOCK, 0o666)
    except OSError:
        pass
    log.info("listening %s -> %s", LISTEN_SOCK, UPSTREAM_SOCK)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            server.server_close()
        finally:
            try:
                os.unlink(LISTEN_SOCK)
            except OSError:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
