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
- With TOKEN_BUDGET set, it tracks tokens sent in the last 60s and holds a
  request only when it would exceed the budget, i.e. it waits *before* a 429
  instead of after one. Under the budget, requests pass straight through.
- If VS Code cancels a request while it is waiting, the request is dropped
  instead of spending tokens on an answer nobody will read.
- Streams successful (SSE) responses straight through.
- Appends one line per chat request (token counts only, never prompt text) to
  a CSV usage log.
- Python 3.7+ standard library only: no pip, no sudo.

Usage:
  UPSTREAM="https://<your-llm-host>" TOKEN_BUDGET=1400000 python3 copilot_retry_proxy.py
Optional env vars: TOKEN_BUDGET (0 = no pacing), MAX_RETRIES (8),
  MAX_WAIT (60s cap per wait), MIN_WAIT (0s floor per retry wait), PORT (8765),
  USAGE_LOG (~/.local/state/copilot-retry-proxy/usage.csv, "" to disable)
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
TOKEN_BUDGET = int(os.environ.get("TOKEN_BUDGET", "0"))  # tokens per WINDOW; 0 = no pacing
USAGE_LOG = os.path.expanduser(
    os.environ.get("USAGE_LOG", "~/.local/state/copilot-retry-proxy/usage.csv"))
WINDOW = 60.0
JITTER = 3.0  # spread out requests released from a shared cooldown
RETRY_CODES = {429, 500, 502, 503, 504}
HOP_HEADERS = {
    "host", "content-length", "connection", "keep-alive", "transfer-encoding",
    "proxy-connection", "te", "upgrade", "accept-encoding",
}
RETRY_RE = re.compile(rb'"retry_after(?:_seconds)?"\s*:\s*([\d.]+)')
MODEL_RE = re.compile(rb'"model"\s*:\s*"([^"]*)"')
PROMPT_RE = re.compile(rb'"prompt_tokens"\s*:\s*(\d+)')
COMPLETION_RE = re.compile(rb'"completion_tokens"\s*:\s*(\d+)')
USAGE_HEADER = "time,model,status,prompt_tokens,completion_tokens,est_tokens,retries,waited_s,total_s\n"
UPSTREAM_ERRORS = (OSError, http.client.HTTPException)

_cooldown_lock = threading.Lock()
_cooldown_until = 0.0  # time.monotonic() before which nobody calls upstream
_usage_lock = threading.Lock()


class ClientGone(Exception):
    """VS Code closed the connection while we were waiting to send upstream."""


class TokenBudget:
    """Sliding window of tokens sent upstream, so we can wait before a 429."""

    def __init__(self, limit):
        self.limit = limit
        self.lock = threading.Lock()
        self.entries = []  # [sent_at, tokens], oldest first; mutable so usage can correct it

    def reserve(self, tokens):
        """Return (entry, 0) if the request fits now, else (None, seconds to wait)."""
        with self.lock:
            now = time.monotonic()
            self.entries = [e for e in self.entries if e[0] > now - WINDOW]
            # A single request bigger than the whole budget only waits for an empty window.
            need = sum(n for _, n in self.entries) + min(tokens, self.limit) - self.limit
            if need <= 0:
                entry = [now, tokens]
                self.entries.append(entry)
                return entry, 0.0
            for sent_at, n in self.entries:  # oldest first: when has enough aged out?
                need -= n
                if need <= 0:
                    return None, sent_at + WINDOW - now


BUDGET = TokenBudget(TOKEN_BUDGET) if TOKEN_BUDGET > 0 else None


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


def parse_usage(data):
    """(prompt_tokens, completion_tokens) from the end of a response, or None."""
    prompt = PROMPT_RE.findall(data)
    if not prompt:
        return None
    completion = COMPLETION_RE.findall(data)
    return int(prompt[-1]), int(completion[-1]) if completion else 0


def record_usage(row):
    if not USAGE_LOG:
        return
    try:
        with _usage_lock:
            new = not os.path.exists(USAGE_LOG)
            with open(USAGE_LOG, "a") as f:
                if new:
                    f.write(USAGE_HEADER)
                f.write(",".join(str(v) for v in row) + "\n")
    except OSError as e:
        log(f"can't write usage log ({e})")


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        started = time.monotonic()
        self.retries, self.waited, self.streamed = 0, 0.0, False
        est = len(body or b"") // 4  # rough prompt size; corrected by real usage afterwards
        entry, status, usage = None, "cancelled", None
        try:
            if BUDGET and body:
                entry = self.pace(est)
            status, usage = self.send_upstream(body)
        except ClientGone:
            log("VS Code cancelled the request while it was waiting, dropped it")
            self.close_connection = True
        if entry is not None:
            if usage:
                entry[1] = usage[0] + usage[1]
            elif not self.streamed:
                entry[1] = 0  # rejected upstream or never sent: it didn't use the budget
        if self.command == "POST":
            m = MODEL_RE.search(body or b"")
            model = m.group(1).decode(errors="replace").replace(",", ";") if m else ""
            prompt, completion = usage or ("", "")
            record_usage((time.strftime("%Y-%m-%dT%H:%M:%S"), model, status, prompt, completion,
                          est, self.retries, round(self.waited, 1),
                          round(time.monotonic() - started, 1)))

    def pace(self, est):
        held = False
        while True:
            entry, wait = BUDGET.reserve(est)
            if entry:
                return entry
            if not held:
                log(f"pacing: ~{est} tokens would exceed {TOKEN_BUDGET}/min, holding {wait:.1f}s")
                held = True
            self.pause(wait + 0.1)

    def send_upstream(self, body):
        """Forward with retries; returns (status, usage) for the usage log."""
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
        url = UPSTREAM + self.path

        for attempt in range(MAX_RETRIES + 1):
            gate = cooldown_remaining()
            if gate > 0:
                self.pause(gate + random.uniform(0, JITTER))
            req = urllib.request.Request(url, data=body, headers=headers, method=self.command)
            try:
                resp = urllib.request.urlopen(req, timeout=600)
            except urllib.error.HTTPError as e:
                err_body = e.read()
                if e.code in RETRY_CODES and attempt < MAX_RETRIES:
                    w = wait_seconds(e.headers, err_body, attempt)
                    log(f"{e.code} from upstream, retry {attempt + 1}/{MAX_RETRIES} in {w:.1f}s")
                    self.retries += 1
                    if e.code == 429:
                        set_cooldown(w)  # waited out at the top of the loop, by every request
                    else:
                        self.pause(w)
                    continue
                log(f"{e.code} from upstream, giving up")
                self.relay_body(e.code, e.headers, err_body)
                return str(e.code), None
            except UPSTREAM_ERRORS as e:  # connection errors, timeouts
                if attempt < MAX_RETRIES:
                    w = min(2 ** attempt, MAX_WAIT)
                    log(f"network error ({e!r}), retry {attempt + 1}/{MAX_RETRIES} in {w:.1f}s")
                    self.retries += 1
                    self.pause(w)
                    continue
                self.relay_body(502, None, f"proxy: upstream unreachable: {e!r}".encode())
                return "unreachable", None
            # Outside the try: once headers go out to VS Code we must never retry.
            return self.relay_stream(resp)

    def pause(self, seconds):
        """Sleep in short steps; raise ClientGone if VS Code hangs up meanwhile."""
        end = time.monotonic() + seconds
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return
            step = min(left, 1.0)
            time.sleep(step)
            self.waited += step
            if self.client_gone():
                raise ClientGone

    def client_gone(self):
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and not self.connection.recv(1, socket.MSG_PEEK)
        except OSError:
            return True

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
        self.streamed = True
        self.send_response(resp.status)
        self.send_upstream_headers(resp.headers)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        status, tail = str(resp.status), b""
        try:
            while True:
                chunk = resp.read1(65536)
                if not chunk:
                    break
                self.wfile.write(b"%X\r\n%s\r\n" % (len(chunk), chunk))
                self.wfile.flush()
                tail = (tail + chunk)[-16384:]  # usage arrives in the last chunk
            if resp.length:  # upstream closed before sending its full Content-Length
                raise http.client.IncompleteRead(b"", resp.length)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except UPSTREAM_ERRORS as e:
            # VS Code cancelled, or upstream died mid-stream. Either way, close
            # without the final chunk so VS Code sees an error, not a cut-off
            # answer that looks complete.
            if isinstance(e, (BrokenPipeError, ConnectionResetError)):
                status = "client-closed"
            else:
                log(f"upstream stream broke mid-response ({e!r})")
                status = "stream-broken"
            self.close_connection = True
        finally:
            resp.close()
        return status, parse_usage(tail)

    def log_message(self, fmt, *args):
        pass  # keep the console quiet except for retries


if __name__ == "__main__":
    if not UPSTREAM:
        sys.exit("Set UPSTREAM to your API base URL, e.g. UPSTREAM=https://host python3 copilot_retry_proxy.py")
    if USAGE_LOG:
        try:
            os.makedirs(os.path.dirname(USAGE_LOG) or ".", exist_ok=True)
        except OSError as e:
            log(f"can't create usage log dir ({e})")
    pacing = f"pacing to {TOKEN_BUDGET} tokens/min" if BUDGET else "no pacing (set TOKEN_BUDGET)"
    log(f"retry proxy on http://127.0.0.1:{PORT} -> {UPSTREAM}, {pacing}")
    try:
        ThreadingHTTPServer(("127.0.0.1", PORT), Proxy).serve_forever()
    except KeyboardInterrupt:
        pass
