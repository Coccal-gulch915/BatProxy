Now I'll create a comprehensive README for your BatProxy project:

***

# 🦇 BatProxy

**BatProxy** is a high-performance, multi-worker proxy system that tunnels HTTP and SOCKS5 traffic through Cloudflare Workers using WebSocket connections. It features intelligent worker selection, real-time health monitoring, adaptive load balancing, and a live dashboard for operational visibility.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [How It Works](#how-it-works)
  - [Authentication Mechanism](#authentication-mechanism)
  - [Worker Selection Algorithm](#worker-selection-algorithm)
  - [Connection Flow](#connection-flow)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Dashboard](#dashboard)
- [Command-Line Options](#command-line-options)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)
- [Security Considerations](#security-considerations)

***

## Overview

BatProxy consists of two main components:

1. **Cloudflare Worker (Server-side)**: A WebSocket-based TCP proxy that accepts authenticated connections and forwards traffic to arbitrary TCP destinations.
2. **Python Client (Local proxy)**: A local HTTP/SOCKS5 proxy server that intelligently routes traffic through multiple Cloudflare Workers with adaptive load balancing, health checks, and failover.

This architecture enables you to bypass network restrictions, distribute traffic across multiple edge locations, and maintain high availability even when individual workers experience issues.

***

## Architecture

```
┌──────────────┐      ┌─────────────────────────────────────┐      ┌──────────────┐
│   Client     │      │        BatProxy Local Server        │      │  Cloudflare  │
│ (Browser/    │─────▶│  (HTTP + SOCKS5 on 127.0.0.1:1080)  │─────▶│   Workers    │
│  curl/APP)   │      │                                     │      │  (WebSocket) │
└──────────────┘      └─────────────────────────────────────┘      └──────────────┘
                                                                       │
                                                                       │ TCP
                                                                       ▼
                                                                ┌──────────────┐
                                                                │   Target     │
                                                                │   Server     │
                                                                │ (example.com)│
                                                                └──────────────┘
```

The local server maintains a pool of Cloudflare Workers, continuously monitoring their health and performance. It uses an adaptive scoring system to select the best worker for each connection while automatically handling failures and cooldowns.

***

## Features

- **Dual Protocol Support**: Seamless handling of both HTTP (including CONNECT) and SOCKS5 proxy protocols
- **Multi-Worker Pool**: Configure multiple Cloudflare Workers with automatic failover
- **Adaptive Load Balancing**: EWMA-based scoring system considering success rate, RTT, and consecutive failures
- **Real-Time Health Checks**: Periodic ping probes to detect and recover failed workers
- **Connection Coalescing**: Batches small WebSocket messages to reduce overhead (4ms window, max 64KB)
- **Nonce-Based Replay Protection**: Prevents authentication token reuse within a 30-second window
- **Live Dashboard**: Rich terminal UI (with `rich` library) or web-based dashboard at `http://127.0.0.1:8088`
- **Destination Caching**: Remembers successful worker-destination pairs for 10 minutes to improve routing
- **Exponential Backoff**: Failed workers enter cooldown with exponential backoff (5s base, 120s max)
- **uvloop Support**: Optional high-performance event loop for improved throughput

***

## How It Works

### Authentication Mechanism

BatProxy uses HMAC-SHA256-based authentication with timestamp and nonce to prevent replay attacks:

1. **Client generates auth token**:
   ```python
   ts = current_timestamp()
   nonce = random_hex(16)
   message = f"{subject}:{ts}:{nonce}"
   sig = HMAC_SHA256(password, message)
   auth = {"ts": ts, "nonce": nonce, "sig": sig}
   ```

2. **Worker validates**:
   - Checks timestamp is within ±30 seconds (`AUTH_WINDOW`)
   - Verifies HMAC signature matches
   - Ensures nonce hasn't been seen before (stored in `seenNonces` Map, max 5000 entries)
   - Rejects if any check fails

The `subject` field is either `"ping"` for health checks or `"{hostname}:{port}"` for actual connections, binding the auth token to a specific target.

### Worker Selection Algorithm

Workers are scored using a weighted formula:

```
score = ewma_success - (ewma_rtt / 10) - slow_penalty
```

Where:
- **ewma_success**: Exponentially weighted moving average of success rate (0–100), α=0.35
- **ewma_rtt**: EWMA of round-trip time in milliseconds, α=0.35
- **slow_penalty**: 20.0 if RTT > 600ms for 3 consecutive times, else 0

**Selection process**:
1. Filter out workers in cooldown (state=`open` with active timer)
2. Sort by: (is_at_max_connections DESC, score DESC)
3. Prefer cached worker for destination if still valid (TTL=600s)
4. Try up to 4 workers per request before failing
5. On success: update EWMA metrics, reset consecutive failures
6. On failure: increment consecutive failures, trigger cooldown after 3 failures

**State machine**:
- `closed`: Healthy, accepting connections
- `half_open`: 1–2 consecutive failures, reduced preference
- `open`: ≥3 consecutive failures, in cooldown (exponential backoff)

### Connection Flow

#### HTTP Request (non-CONNECT)

1. Client connects to local proxy on port 1080
2. Proxy parses HTTP headers to extract `Host` and target
3. Selects best worker via `open_tunnel()`
4. Sends full HTTP request through WebSocket tunnel
5. Relays response back to client
6. For GET/HEAD: may retry on different worker if first fails

#### HTTP CONNECT (TLS tunneling)

1. Client sends `CONNECT example.com:443`
2. Proxy establishes WebSocket tunnel to worker
3. Responds `HTTP/1.1 200 Connection Established`
4. Bidirectional byte-stream relay between client and target
5. TLS handshake occurs directly between client and target (proxy is transparent)

#### SOCKS5

1. Client initiates SOCKS5 handshake (version byte `0x05`)
2. Proxy accepts with no-auth (`0x05 0x00`)
3. Client sends CONNECT command with target address
4. Proxy establishes tunnel and responds with success (`0x05 0x00 0x01...`)
5. Bidirectional relay begins

***

## Installation

### Prerequisites

- Python 3.8+
- Cloudflare account (for deploying Workers)
- Optional: `rich` library for colored terminal dashboard
- Optional: `uvloop` for performance boost

### 1. Deploy Cloudflare Worker

Create a new Worker in your Cloudflare dashboard or via CLI:

```bash
# Using Wrangler CLI
wrangler init batproxy-worker
cd batproxy-worker
```

Replace `src/index.js` with the Worker code (first code block in your snippet).

Set the password (optional, defaults to `123456`):

```bash
wrangler secret put PASSWD
# Enter your password when prompted
```

Deploy:

```bash
wrangler deploy
```

Note the Worker URL (e.g., `https://batproxy-worker.your-subdomain.workers.dev`).

### 2. Install Python Dependencies

```bash
pip install websockets rich uvloop
```

Or minimal installation:

```bash
pip install websockets
```

### 3. Configure Workers List

Edit the `WORKERS` list in the Python script:

```python
WORKERS = [
    {"url": "wss://batproxy-worker.your-subdomain.workers.dev", "password": "your_password"},
    {"url": "wss://backup-worker.another-subdomain.workers.dev", "password": "backup_password"},
    # Add more workers for redundancy
]
```

***

## Configuration

### Key Parameters (Python Client)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LOCAL_HOST` | `127.0.0.1` | Local proxy bind address |
| `LOCAL_PORT` | `1080` | Local proxy port |
| `DASHBOARD_HOST` | `127.0.0.1` | Web dashboard bind address |
| `DASHBOARD_PORT` | `8088` | Web dashboard port |
| `CONNECT_TIMEOUT` | `6` | Seconds to wait for worker connection |
| `MAX_ATTEMPTS_PER_REQUEST` | `4` | Max workers to try before failing |
| `HEALTH_CHECK_INTERVAL` | `30` | Seconds between health check cycles |
| `HALF_OPEN_AFTER_FAILS` | `3` | Consecutive failures before cooldown |
| `MAX_CONN_PER_WORKER` | `150` | Max concurrent connections per worker |
| `DEST_CACHE_TTL` | `600` | Destination cache TTL in seconds |
| `ALPHA_SUCCESS` | `0.35` | EWMA smoothing factor for success rate |
| `ALPHA_RTT` | `0.35` | EWMA smoothing factor for RTT |
| `SLOW_RTT_MS` | `600` | RTT threshold for slow penalty |
| `SLOW_PENALTY` | `20.0` | Score penalty for slow workers |
| `RETRYABLE_METHODS` | `{"GET", "HEAD"}` | HTTP methods safe to retry on failure |

### Key Parameters (Cloudflare Worker)

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_PASSWORD` | `"123456"` | Fallback password if `PASSWD` secret not set |
| `AUTH_WINDOW` | `30` | Seconds of timestamp tolerance |
| `COALESCE_MS` | `4` | Milliseconds to buffer before flushing WebSocket |
| `COALESCE_MAX` | `65536` | Max bytes before forced flush |
| `MAX_HANDSHAKE_BYTES` | `2048` | Max size of initial JSON handshake |
| `PING_TARGET_HOST` | `"1.1.1.1"` | Host used for ping health checks |
| `PING_TARGET_PORT` | `443` | Port used for ping health checks |
| `MAX_NONCES` | `5000` | Max nonces to track for replay protection |

***

## Usage

### Start the Proxy

```bash
python batproxy.py
```

With verbose logging:

```bash
python batproxy.py --verbose
```

### Using with curl

**HTTP proxy mode**:

```bash
curl -x http://127.0.0.1:1080 https://example.com
```

**SOCKS5 proxy mode**:

```bash
curl -x socks5h://127.0.0.1:1080 https://example.com
```

### Browser Configuration

Configure your browser to use:
- **HTTP Proxy**: `127.0.0.1:1080`
- **SOCKS5 Proxy**: `127.0.0.1:1080`

All protocols (HTTP, HTTPS, WebSocket, etc.) are supported through the same port.

### Programmatic Usage

```python
import requests

proxies = {
    "http": "http://127.0.0.1:1080",
    "https": "http://127.0.0.1:1080",
}

response = requests.get("https://api.example.com/data", proxies=proxies)
```

***

## Dashboard

### Terminal Dashboard (Rich)

If `rich` is installed, a live dashboard updates 4 times per second:

```
┌────────────────────────────────────────────────────────────┐
│ 🦇 Bat Proxy                                               │
├──────────┬─────────┬────────┬─────────┤
│ Active   │ Total   │ OK     │ Failed  │
│ 12       │ 1547    │ 1520   │ 27      │
└──────────┴─────────┴────────┴─────────┘

┌────────────────────────────────────────────────────────────┐
│ Workers                                                    │
├──────────────┬──────────┬───────┬────────┬───────┬────┬────┤
│ Worker       │ Status   │ Conns │ RTT    │ Score │ OK │Fail│
├──────────────┼──────────┼───────┼────────┼───────┼────┼────┤
│ jolly-sunset │ closed   │ 5     │ 120ms  │ 75    │ 80 │ 5  │
│ backup-node  │ half-open│ 0     │ 450ms  │ 45    │ 30 │ 10 │
└──────────────┴──────────┴───────┴────────┴───────┴────┴────┘
```

### Web Dashboard

Access at `http://127.0.0.1:8088` for a modern, responsive UI with:
- Live stats (active, total, ok, failed)
- Per-worker status, RTT, score, and connection count
- Auto-refresh every second
- Visual score bar (gradient from accent to green)

Disable web dashboard with `--no-web-dashboard`.

***

## Command-Line Options

```
usage: batproxy.py [-h] [-v] [--host HOST] [--port PORT]
                   [--dashboard-host DASHBOARD_HOST]
                   [--dashboard-port DASHBOARD_PORT] [--no-web-dashboard]

Bat Proxy - HTTP + SOCKS5 tunnel via Cloudflare Workers

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Enable verbose logging (shows worker selection)
  --host HOST           Local proxy bind host (default: 127.0.0.1)
  --port PORT           Local proxy port (default: 1080)
  --dashboard-host DASHBOARD_HOST
                        Web dashboard bind host (default: 127.0.0.1)
  --dashboard-port DASHBOARD_PORT
                        Web dashboard port (default: 8088)
  --no-web-dashboard    Disable web dashboard server
```

***

## Performance Tuning

### Enable uvloop

uvloop provides a faster event loop implementation:

```bash
pip install uvloop
python batproxy.py  # automatically uses uvloop if available
```

### Tune Buffer Parameters

For high-throughput scenarios:

```python
WRITE_BUFFER_MAX = 131072  # Increase from 64KB to 128KB
WRITE_BUFFER_DELAY = 0.002  # Reduce from 4ms to 2ms
```

### Adjust Worker Limits

If you have many concurrent connections:

```python
MAX_CONN_PER_WORKER = 300  # Increase from 150
```

Monitor worker performance and adjust `ALPHA_SUCCESS` and `ALPHA_RTT` for more aggressive or conservative scoring.

### Connection Coalescing

The Worker uses a 4ms coalesce window to batch small messages. For latency-sensitive workloads, reduce `COALESCE_MS` to `1` or `0` (but expect higher overhead).

***

## Troubleshooting

### All workers failed: ConnectionError

- **Cause**: All workers are in cooldown or unreachable
- **Solution**: 
  - Check Cloudflare Worker status in dashboard
  - Verify Worker URLs and passwords in `WORKERS` list
  - Increase `CONNECT_TIMEOUT` if network is slow
  - Add more workers to the pool

### High RTT scores

- **Cause**: Workers responding slowly (>600ms)
- **Solution**:
  - Deploy Workers in regions closer to your targets
  - Check Cloudflare Worker execution logs for errors
  - Reduce `MAX_CONN_PER_WORKER` to avoid overload

### Authentication failed

- **Cause**: Password mismatch or clock skew
- **Solution**:
  - Ensure Worker `PASSWD` secret matches client password
  - Check system clock synchronization (auth window is ±30s)
  - Verify HMAC implementation (should use SHA-256)

### Proxy slow or unresponsive

- **Cause**: Buffer buildup or event loop starvation
- **Solution**:
  - Install `uvloop` for better performance
  - Increase `WRITE_BUFFER_MAX`
  - Reduce `MAX_CONN_PER_WORKER`
  - Check for CPU or memory pressure on local machine

### Dashboard not showing

- **Cause**: Port conflict or `rich` not installed
- **Solution**:
  - Change `DASHBOARD_PORT` to avoid conflicts
  - Install `rich`: `pip install rich`
  - Use `--no-web-dashboard` and rely on terminal output

***

## Security Considerations

### Authentication

- **Use strong passwords**: Change `DEFAULT_PASSWORD` via `PASSWD` secret
- **Rotate secrets regularly**: Update both Worker and client configs simultaneously
- **HMAC provides integrity**: Prevents tampering, but not encryption

### Transport Security

- **WebSocket traffic is not encrypted**: Between client and Worker, data is visible to Cloudflare
- **Use TLS for sensitive data**: End-to-end encryption (HTTPS) protects payload
- **Consider Cloudflare's Zero Trust**: For additional encryption layers

### Replay Protection

- **Nonces are single-use**: Within 30-second window, max 5000 tracked
- **Timestamp validation**: Rejects tokens older than 30 seconds
- **Clock synchronization**: Ensure client and server clocks are within 30s

### Rate Limiting

- **No built-in rate limiting**: Add Cloudflare Rules or
