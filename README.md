# copilot-retry-proxy

A tiny local proxy that makes VS Code Copilot Chat **automatically retry** when a
Custom Endpoint (any OpenAI-compatible API) answers with a rate limit (`429`) or a
transient `5xx`, so you never have to click **Try Again**.

```
VS Code Copilot Chat  ──►  127.0.0.1:8765 (this proxy)  ──►  your LLM API
                              │ 429? wait & retry
```

- **Single file, Python 3.7+ standard library only.** No pip, no sudo.
- **Honors the server's hint.** It uses `Retry-After`, or `retry_after` / `retry_after_seconds`
  in the error body, and falls back to exponential backoff otherwise.
- **Shared cooldown.** A 429 on one request pauses every in-flight request, and each one
  adds a little jitter when it resumes. Copilot's parallel requests don't all
  hit the limit again the moment the wait ends.
- **Respects cancel.** If you stop a request in VS Code while it is waiting, the retry
  is dropped, so you don't spend tokens on an answer nobody will read.
- **Streams responses** (SSE) straight through. If the upstream dies mid-stream, VS Code
  gets an error instead of a cut-off answer that looks complete.
- **Local only.** It binds to `127.0.0.1` and passes your `Authorization` header through
  unchanged. It never stores your API key.

## Usage

```sh
UPSTREAM="https://your-llm-host" MIN_WAIT=30 python3 copilot_retry_proxy.py
```

Then point your Custom Endpoint (e.g. in `chatLanguageModels.json`) at the proxy instead of the API:

```diff
- https://your-llm-host/v1/chat/completions
+ http://127.0.0.1:8765/v1/chat/completions
```

Keep the same API key.

| Env var       | Default | Meaning |
|---------------|---------|---------|
| `UPSTREAM`    | —       | Base URL of the real API (required) |
| `PORT`        | `8765`  | Local port to listen on |
| `MAX_RETRIES` | `8`     | Retries before the error is passed to VS Code |
| `MIN_WAIT`    | `0`     | Minimum seconds per retry wait (e.g. `30` for a per-minute token budget) |
| `MAX_WAIT`    | `60`    | Maximum seconds per retry wait |

The console stays quiet except for retry and error lines.

## License

MIT
