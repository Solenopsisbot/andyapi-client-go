# AndyAPI Local Provider Client

Bridge your local LLMs (OpenAI-compatible) to the AndyAPI server over WebSocket, with a tiny built-in management UI for setup, model discovery, and connection control.

- Lightweight Go binary (no external DB)
- WebSocket heartbeat + exponential reconnect
- Multiple API endpoints with custom headers/parameters
- Model ID mapping (different internal/external names)
- Statistics tracking for requests, latency, and tokens
- Hot reload - config changes apply without restart
- Configurable timeouts per endpoint or model
- Local model discovery via `/v1/models` (Ollama, vLLM, OpenAI-compatible APIs)
- Forwards chat completions to your local API (`/v1/chat/completions`)
- Built-in UI at `http://localhost:8090/ui` by default

---

## Quick start

1) Build or run

- Run directly (requires Go 1.22+):

```fish
cd client
go run .
```

- Or build for your OS:

```fish
cd client
go build -o andyapi-client
./andyapi-client
```

2) Open the UI

- Visit http://localhost:8090/ui
- If no `config.yaml` exists, the client starts in "initial setup" mode.

3) Fill in settings

- Andy API Base URL (**correct:** `https://andy.mindcraft-ce.com/api`; example: `http://localhost:8080` or `https://api.example.com`)  → the client derives `ws(s)://…/ws`
- Optional Provider name (default: `provider`)
- Configure one or more API endpoints (Ollama, OpenRouter, vLLM, etc.)

4) Discover and enable models

- Click "Scan from Local API" to populate models from `GET {endpoint_url}/v1/models`
- Toggle "Enabled" for models you want to serve
- Optionally set different internal/external model IDs
- Save

5) Connect to AndyAPI

- Click "Connect to AndyAPI" in the header.
- Status badge shows: never connected → connected/disconnected

---

## What it does

```
AndyAPI  ⇄  WebSocket (/ws)
   ⇅           ↑ heartbeat/ping
Client
   ⇅ request/response
Multiple OpenAI-compatible APIs
├─ Ollama (local)
├─ vLLM (local)
├─ OpenRouter (remote)
└─ Any OpenAI-compatible endpoint
        ↳ /v1/chat/completions
```

- On connect, the client registers the models you enabled.
- Incoming requests from AndyAPI are forwarded to the appropriate endpoint's `/v1/chat/completions` with the configured model ID.
- Model ID mapping allows exposing a friendly name while using the actual model ID internally.
- Responses are streamed back (non-streaming payload today; first choice content is returned).
- Statistics are tracked per model: requests, latency, token usage.

---

## Configuration

Location: `client/config.yaml` (auto-created/saved from the UI). Example fields:

```yaml
andy_api_url: "https://andy.mindcraft-ce.com/api"
client_token: ""
provider: "local-llm"
heartbeat_interval: 30
reconnect_max_backoff: 30
default_timeout: 120          # Global default timeout in seconds
hot_reload: true              # Reload config when file changes

# Multiple API endpoints
endpoints:
  - id: "local-ollama"
    name: "Local Ollama"
    base_url: "http://localhost:11434/v1"
    api_key: ""
    timeout: 120              # Per-endpoint timeout
    enabled: true
    priority: 1               # Lower = higher priority for fallback
    headers: {}               # Extra HTTP headers
    extra_params: {}          # Extra request parameters
    
  - id: "openrouter"
    name: "OpenRouter"
    base_url: "https://openrouter.ai/api/v1"
    api_key: "sk-or-..."
    timeout: 180
    enabled: false
    priority: 2
    headers:
      HTTP-Referer: "https://andy.mindcraft-ce.com"
      X-Title: "AndyAPI"
    extra_params:
      transforms: ["middle-out"]

# Legacy single endpoint (for backward compatibility)
local_api_url: ""
local_api_key: ""

# Models with ID mapping and per-model settings
models:
  - name: "gpt-4-local"           # External name (exposed to AndyAPI)
    upstream_id: "llama3.2:latest" # Internal model ID (used with endpoint)
    endpoint_id: "local-ollama"    # Which endpoint to use
    max_completion_tokens: 4096
    concurrent_connections: 4
    supports_embedding: false
    supports_vision: false
    fallback: false
    enabled: true
    timeout: 0                     # Per-model timeout (0 = use endpoint default)
    headers: {}                    # Extra headers for this model
    extra_params:                  # Extra params for this model
      temperature: 0.7
```

Notes
- **The correct AndyAPI base URL is:** `https://andy.mindcraft-ce.com/api`
- `andy_api_url` can be `http(s)://…` or `ws(s)://…`; if `http(s)://`, the client derives `ws(s)://…/ws` automatically.
- Model discovery uses `GET {endpoint_url}/v1/models` and heuristically marks embedding/vision when names include keywords like "embed", "vision", or "vl".
- **Model ID Mapping**: Use `upstream_id` to specify the actual model name used when calling the API. The external `name` is what AndyAPI sees.
- **Timeout hierarchy**: Model timeout > Endpoint timeout > Global default > 120s fallback

---

## Management UI and API

UI routes
- `GET /ui` – SPA for configuration and controls

Health and state
- `GET /health` – `{ status, client_id, ws_url }`
- `GET /api/status` – `{ connected, ever_connected, client_id, ws_url }`
- `GET /api/state` – `{ initial_setup }`
- `GET /config` – Current config as JSON

Connection control
- `POST /api/connect` – Start background WS connect loop
- `POST /api/disconnect` – Stop loop and close the socket

Config endpoints
- `POST /api/save-config` – Only during initial setup; saves full config
- `POST /api/update-config` – Update config after setup (URL, provider, endpoints, models, etc.)
- `POST /api/reload-config` – Manually reload config from disk
- `POST /models` – Replace models (array) and broadcast update to server

Endpoint management
- `GET /api/endpoints` – List all configured endpoints
- `POST /api/endpoints` – Replace all endpoints
- `POST /api/endpoints/add` – Add a new endpoint
- `DELETE /api/endpoints/:id` – Delete an endpoint by ID

Model scanning
- `POST /api/scan-models` – Body: `{ base_url, api_key, endpoint_id }`; responds `{ models: [...] }`

Statistics
- `GET /api/stats` – All model statistics
- `GET /api/stats/:model` – Statistics for a specific model
- `POST /api/stats/reset` – Reset statistics (optional body: `{ model: "name" }` to reset single model)

Default listen address: `:8090` (change with `-http` flag).

---

## Command-line flags

- `-config <path>` – Path to config YAML (default `config.yaml`)
- `-example <path>` – Example config to load if `-config` missing (default `config.example.yaml`)
- `-http <addr>` – Management HTTP listen address (default `:8090`)

Examples

```fish
# Custom config and port
./andyapi-client -config ./my-config.yaml -http :8089
```

---

## Build options

Simple build

```fish
cd client
go build -o andyapi-client
```

Cross-platform build script

- Script: `client/build.sh` (bash)
- Outputs to `client/dist/<goos>-<goarch>/`
- Variables:
  - `VERSION` (default: `dev`)
  - `TARGETS` (default: `"windows/amd64 linux/amd64"`)
  - `CLEAN` (default: `false`)
  - `RACE` (default: `false`)
  - `VERBOSE` (default: `false`)

Example

```fish
cd client
env VERSION=0.1.0 TARGETS="linux/amd64" bash ./build.sh
```

Artifacts include a binary, `SHA256SUMS`, and a `.tar.gz` bundle.

---

## Troubleshooting

- Not connecting
  - Check AndyAPI base URL; HTTPS → WSS, HTTP → WS (client derives `/ws`).
  - Verify the server exposes `/ws` and is reachable from this machine.
  - See `/api/status` and logs for errors; adjust `heartbeat_interval`/`reconnect_max_backoff`.

- Model scan fails
  - Ensure the endpoint `base_url` is correct and reachable; it must expose `/v1/models`.
  - If your endpoint requires a key, set `api_key` on the endpoint.

- Completions fail
  - Ensure the `upstream_id` (or `name` if not mapped) exists in your endpoint and supports chat completions.
  - Check the Statistics tab for error details.
  - The client uses the first choice's message content from `/v1/chat/completions` responses.

- Port in use
  - Change the management address via `-http :<port>`.

- Config not reloading
  - Ensure `hot_reload: true` in your config
  - Check logs for reload errors

---

## Security notes

- API keys are stored locally in `config.yaml`; secure file permissions as needed.
- When exposing the UI remotely, protect the port with firewall/auth (no built-in auth yet).
- Prefer `https://` for AndyAPI so the client uses `wss://`.

---

## Requirements

- Go 1.22 or newer (to build from source)
- A local OpenAI-compatible API for inference:
  - Examples: Ollama, vLLM, LM Studio, OpenWebUI in OpenAI proxy mode
- A public OpenAI-compatible API for inference (Optional):
  - Examples: OpenRouter, OpenAI, TogetherAI, Gemini

---

## License

This client is part of the AndyAPI project. See the repository’s main license for details.
