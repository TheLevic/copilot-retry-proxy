# copilot-retry-proxy

A tiny local proxy that makes VS Code Copilot Chat **automatically retry** when a
Custom Endpoint (any OpenAI-compatible API) answers with a rate limit (`429`) or a
transient `5xx`, so you never have to click **Try Again**.

```
VS Code Copilot Chat  ──►  127.0.0.1:8765 (this proxy)  ──►  your LLM API
                              │ over budget? hold briefly
                              │ 429? wait & retry
```

- **Single file, Python 3.7+ standard library only.** No pip, no sudo.
- **Waits before a 429, not after it.** With `TOKEN_BUDGET` set, it counts tokens sent
  in the last 60s and holds a request only if it would go over. Under the budget,
  requests go straight through.
- **Honors the server's hint.** It uses `Retry-After`, or `retry_after` / `retry_after_seconds`
  in the error body, and falls back to exponential backoff otherwise.
- **Shared cooldown.** A 429 on one request pauses every in-flight request, and each one
  adds a little jitter when it resumes. Copilot's parallel requests don't all
  hit the limit again the moment the wait ends.
- **Respects cancel.** If you stop a request in VS Code while it is waiting, it is dropped,
  so you don't spend tokens on an answer nobody will read.
- **Streams responses** (SSE) straight through. If the upstream dies mid-stream, VS Code
  gets an error instead of a cut-off answer that looks complete.
- **Usage log.** It writes one CSV line per chat request with token counts only and never
  prompt text.
- **Local only.** It binds to `127.0.0.1` and passes your `Authorization` header through
  unchanged. It never stores your API key.

## Quick start

```sh
UPSTREAM="https://your-llm-host" TOKEN_BUDGET=1400000 MIN_WAIT=15 python3 copilot_retry_proxy.py
```

Then point your Custom Endpoint (e.g. in `chatLanguageModels.json`) at the proxy instead of the API:

```diff
- https://your-llm-host/v1/chat/completions
+ http://127.0.0.1:8765/v1/chat/completions
```

Keep the same API key.

## Run it automatically (systemd user service, no sudo)

The service starts the proxy when you log in and restarts it if it ever exits.

```sh
git clone https://github.com/TheLevic/copilot-retry-proxy ~/copilot-retry-proxy
mkdir -p ~/.config/systemd/user
cp ~/copilot-retry-proxy/copilot-retry-proxy.service ~/.config/systemd/user/
# edit UPSTREAM (and TOKEN_BUDGET) in ~/.config/systemd/user/copilot-retry-proxy.service
systemctl --user daemon-reload
systemctl --user enable --now copilot-retry-proxy
journalctl --user -u copilot-retry-proxy -f   # watch retries / pacing
```

A service does not inherit your shell's environment. If your network needs `HTTPS_PROXY`,
set it in the unit file (there's a commented line for it).

## Settings

| Env var        | Default | Meaning |
|----------------|---------|---------|
| `UPSTREAM`     | —       | Base URL of the real API (required) |
| `TOKEN_BUDGET` | `0`     | Tokens per minute to pace to; `0` turns pacing off. Set it a bit under your real limit (e.g. `1400000` for 1.5M). |
| `MIN_WAIT`     | `0`     | Minimum seconds per retry wait (e.g. `15`) |
| `MAX_WAIT`     | `60`    | Maximum seconds per retry wait |
| `MAX_RETRIES`  | `8`     | Retries before the error is passed to VS Code |
| `PORT`         | `8765`  | Local port to listen on |
| `USAGE_LOG`    | `~/.local/state/copilot-retry-proxy/usage.csv` | Usage CSV; set to an empty value to disable |

**How pacing counts tokens:** a request is first estimated at body size ÷ 4. When the response
includes a `usage` block, that estimate is replaced with the real prompt + completion count.
If your server doesn't report usage, the estimate stands. Either way, the 429 retry
still catches any request that gets through.

## Usage log

```
time,model,status,prompt_tokens,completion_tokens,est_tokens,retries,waited_s,total_s
2026-10-21T08:19:19,my-model,200,48211,912,51034,0,0.0,14.2
```

Tokens per model:

```sh
awk -F, 'NR>1 {t[$2]+=$4+$5} END {for (m in t) print t[m], m}' ~/.local/state/copilot-retry-proxy/usage.csv
```

Seconds spent waiting (pacing + retries):

```sh
awk -F, 'NR>1 {w+=$8} END {print w}' ~/.local/state/copilot-retry-proxy/usage.csv
```

## License

MIT
