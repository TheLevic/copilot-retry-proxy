#!/usr/bin/env python3
"""
Local auto-retry proxy for VS Code Copilot Chat (Custom Endpoint) -> any
OpenAI-compatible API that rate-limits you.

- Listens on 127.0.0.1 only; forwards every request unchanged to UPSTREAM.
- On 429 / 5xx it waits (honoring Retry-After or "retry_after_seconds" in the
  error body, else exponential backoff) and retries, so VS Code never sees the
  failure unless all retries are exhausted.
- A 429 pauses *all* in-flight requests (shared cooldown + jitter), so parallel
  Copilot requests don't stampede the endpoint the moment the wait ends.
- If VS Code cancels a request while it is waiting, the retry is dropped instead
  of spending tokens on an answer nobody will read.
- Streams successful (SSE) responses straight through.
- Python 3.7+ standard library only: no pip, no sudo.

Usage:
  UPSTREAM="https://<your-llm-host>" python3 copilot_retry_proxy.py
Optional env vars: MAX_RETRIES (8), MAX_WAIT (60s cap per wait),
  MIN_WAIT (0s floor per wait, e.g. MIN_WAIT=30 to let the token budget refill),
  PORT (8765)
Then change the Custom Endpoint URL in chatLanguageModels.json from
  https://<your-llm-host>/v1/chat/completions
to
  http://127.0.0.1:8765/v1/chat/completions
(API key stays the same; the proxy passes the Authorization header through.)
"""
import http.client
import os
import random
import re
import select
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ.get("UPSTREAM", "").rstrip("/")
PORT = int(os.environ.get("PORT", "8765"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "8"))
MAX_WAIT = float(os.environ.get("MAX_WAIT", "60"))
MIN_WAIT = float(os.environ.get("MIN_WAIT", "0"))  # floor for every retry wait
JITTER = 3.0  # spread out requests released from a shared cooldown
RETRY_CODES = {429, 500, 502, 503, 504}
HOP_HEADERS = {
    "host", "content-length", "connection", "keep-alive", "transfer-encoding",
    "proxy-connection", "te", "upgrade", "accept-encoding",
}
RETRY_RE = re.compile(rb'"retry_after(?:_seconds)?"\s*:\s*([\d.]+)')
UPSTREAM_ERRORS = (OSError, http.client.HTTPException)

_cooldown_lock = threading.Lock()
_cooldown_until = 0.0  # time.monotonic() before which nobody calls upstream


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, file=sys.stderr, flush=True)


def set_cooldown(seconds):
    global _cooldown_until
    with _cooldown_lock:
        _cooldown_until = max(_cooldown_until, time.monotonic() + seconds)


def cooldown_remaining():
    return max(0.0, _cooldown_until - time.monotonic())


def wait_seconds(headers, body, attempt):
    return min(max(_hint_seconds(headers, body, attempt), MIN_WAIT), MAX_WAIT)


def _hint_seconds(headers, body, attempt):
    hdr = headers.get("Retry-After") if headers else None
    if hdr:
        try:
            return min(float(hdr) + 0.5, MAX_WAIT)
        except ValueError:
            pass
    m = RETRY_RE.search(body or b"")
    if m:
        return min(float(m.group(1)) + 0.5, MAX_WAIT)
    return min(2 ** attempt, MAX_WAIT)


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
        url = UPSTREAM + self.path

        for attempt in range(MAX_RETRIES + 1):
            gate = cooldown_remaining()
            if gate > 0 and not self.pause(gate + random.uniform(0, JITTER)):
                return self.drop()
            req = urllib.request.Request(url, data=body, headers=headers, method=self.command)
            try:
                resp = urllib.request.urlopen(req, timeout=600)
            except urllib.error.HTTPError as e:
                err_body = e.read()
                if e.code in RETRY_CODES and attempt < MAX_RETRIES:
                    w = wait_seconds(e.headers, err_body, attempt)
                    log(f"{e.code} from upstream, retry {attempt + 1}/{MAX_RETRIES} in {w:.1f}s")
                    if e.code == 429:
                        set_cooldown(w)  # waited out at the top of the loop, by every request
                    elif not self.pause(w):
                        return self.drop()
                    continue
                log(f"{e.code} from upstream, giving up")
                return self.relay_body(e.code, e.headers, err_body)
            except UPSTREAM_ERRORS as e:  # connection errors, timeouts
                if attempt < MAX_RETRIES:
                    w = min(2 ** attempt, MAX_WAIT)
                    log(f"network error ({e!r}), retry {attempt + 1}/{MAX_RETRIES} in {w:.1f}s")
                    if not self.pause(w):
                        return self.drop()
                    continue
                return self.relay_body(502, None, f"proxy: upstream unreachable: {e!r}".encode())
            # Outside the try: once headers go out to VS Code we must never retry.
            return self.relay_stream(resp)

    def pause(self, seconds):
        """Sleep in short steps; return False if VS Code hung up meanwhile."""
        end = time.monotonic() + seconds
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return True
            time.sleep(min(left, 1.0))
            if self.client_gone():
                return False

    def client_gone(self):
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and not self.connection.recv(1, socket.MSG_PEEK)
        except OSError:
            return True

    def drop(self):
        log("VS Code cancelled the request, dropping retry")
        self.close_connection = True

    def send_upstream_headers(self, headers):
        if headers:
            for k, v in headers.items():
                if k.lower() not in HOP_HEADERS:
                    self.send_header(k, v)

    def relay_body(self, status, headers, data):
        self.send_response(status)
        self.send_upstream_headers(headers)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def relay_stream(self, resp):
        self.send_response(resp.status)
        self.send_upstream_headers(resp.headers)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            while True:
                chunk = resp.read1(65536)
                if not chunk:
                    break
                self.wfile.write(b"%X\r\n%s\r\n" % (len(chunk), chunk))
                self.wfile.flush()
            if resp.length:  # upstream closed before sending its full Content-Length
                raise http.client.IncompleteRead(b"", resp.length)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except UPSTREAM_ERRORS as e:
            # VS Code cancelled, or upstream died mid-stream. Either way, close
            # without the final chunk so VS Code sees an error, not a cut-off
            # answer that looks complete.
            if not isinstance(e, (BrokenPipeError, ConnectionResetError)):
                log(f"upstream stream broke mid-response ({e!r})")
            self.close_connection = True
        finally:
            resp.close()

    def log_message(self, fmt, *args):
        pass  # keep the console quiet except for retries


if __name__ == "__main__":
    if not UPSTREAM:
        sys.exit("Set UPSTREAM to your API base URL, e.g. UPSTREAM=https://host python3 copilot_retry_proxy.py")
    log(f"retry proxy on http://127.0.0.1:{PORT} -> {UPSTREAM}")
    try:
        ThreadingHTTPServer(("127.0.0.1", PORT), Proxy).serve_forever()
    except KeyboardInterrupt:
        pass
