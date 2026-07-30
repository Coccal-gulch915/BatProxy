<p align="center">
  <a href="README.md">English</a> | <a href="readmafa.md#مرحله-به-مرحله-نصب-و-راه‌اندازی">فارسی</a>
</p>

<p align="center">
  <a href="#-quick-start">🚀 Quick Start</a>
</p>

# 🦇 BatProxy - Enterprise-Grade Intelligent Proxy Tunnel

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange.svg)](https://workers.cloudflare.com/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

![BatProxy Banner](https://github.com/user-attachments/assets/37f66bd2-26ae-4eb8-a6ab-e90e130684a3)

**BatProxy** is a sophisticated, high-performance proxy solution that combines the power of Cloudflare Workers with an intelligent Python client. It creates a resilient, self-healing tunnel network that automatically adapts to network conditions, provides automatic failover, and delivers exceptional throughput through intelligent request routing and data coalescing.

> **⚠️ Important Security Note**: For enhanced privacy, we strongly recommend using **disposable email addresses** (such as ProtonMail or temporary email services) when creating Cloudflare accounts for this project.

## 📋 Table of Contents

- [Banner & Introduction](#-banner--introduction)
- [Key Features](#-key-features)
- [Architecture Diagram](#-architecture-diagram)
- [How It Works](#-how-it-works)
  - [1. Handshake & Authentication Flow](#1-handshake--authentication-flow)
  - [2. Data Relay & Coalescing](#2-data-relay--coalescing)
  - [3. HTTP/HTTPS CONNECT Flow](#3-httphttps-connect-flow)
  - [4. SOCKS5 Flow](#4-socks5-flow)
  - [5. Smart Worker Selection Algorithm](#5-smart-worker-selection-algorithm)
  - [6. Circuit Breaker Algorithm](#6-circuit-breaker-algorithm)
  - [7. Health Check System](#7-health-check-system)
  - [8. Destination Cache](#8-destination-cache)
  - [9. Retry Logic & Idempotency](#9-retry-logic--idempotency)
- [Technical Deep Dive](#-technical-deep-dive)
  - [Cloudflare Worker Internals](#cloudflare-worker-internals)
  - [Python Client Internals](#python-client-internals)
  - [Authentication Mechanism (HMAC + Nonce + Timestamp)](#authentication-mechanism-hmac--nonce--timestamp)
  - [Performance Optimizations](#performance-optimizations)
  - [Security Considerations](#security-considerations)
- [Monitoring & Dashboards](#-monitoring--dashboards)
  - [Terminal Dashboard](#terminal-dashboard)
  - [Web Dashboard](#web-dashboard)
- [Prerequisites & Requirements](#-prerequisites--requirements)
- [Installation Guide](#-installation-guide)
  - [Cloudflare Deployment](#cloudflare-deployment)
  - [Setting PASSWD Secret](#setting-passwd-secret)
  - [Python Installation](#python-installation)
- [Configuration](#-configuration)
  - [Client Configuration](#client-configuration)
  - [Worker Configuration](#worker-configuration)
  - [Environment Variables](#environment-variables)
  - [Command Line Arguments](#command-line-arguments)
- [Usage Examples](#-usage-examples)
  - [Browser Configuration](#browser-configuration)
  - [curl Examples](#curl-examples)
  - [Multi-Worker Configuration](#multi-worker-configuration)
  - [Recommended Settings](#recommended-settings)
- [Troubleshooting Guide](#-troubleshooting-guide)
- [Frequently Asked Questions](#-frequently-asked-questions)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)
- [Credits](#-credits)

---

## 🏆 Banner & Introduction

```
╔════════════════════════════════════════════════════════════════╗
║                    🦇 BATPROXY v2.0                          ║
║        Intelligent Proxy Tunnel for the Modern Web           ║
╠════════════════════════════════════════════════════════════════╣
║  HTTP/HTTPS  │  SOCKS5  │  Auto-Failover  │  Load Balancing  ║
║  Circuit Breaker  │  Health Checks  │  Real-time Dashboard  ║
╚════════════════════════════════════════════════════════════════╝
```

BatProxy is not just another proxy tool—it's a complete traffic routing ecosystem designed for reliability, performance, and ease of use. Whether you're a developer needing to bypass geographic restrictions, a security researcher testing network configurations, or simply someone who values privacy, BatProxy provides enterprise-grade features in a lightweight, easy-to-deploy package.

The system leverages Cloudflare's global edge network to provide low-latency connections worldwide, while the intelligent Python client ensures optimal worker selection, automatic recovery from failures, and seamless failover between multiple workers.

---

## ✨ Key Features

### Core Capabilities
- **Dual Protocol Support**: Full-featured HTTP/HTTPS proxy with CONNECT method support for TLS tunnels, plus complete SOCKS5 protocol implementation
- **Intelligent Worker Selection**: Multi-factor scoring system that considers success rates, latency, active connections, and recent failure history
- **Automatic Circuit Breaking**: Workers that become unhealthy are temporarily excluded, with exponential backoff cooldown periods
- **Zero-Downtime Failover**: Requests automatically retry through alternative workers when failures occur
- **Idempotent Request Retry**: Automatic retry for GET, HEAD, and other idempotent methods with transparent failover

### Performance Optimizations
- **Data Coalescing**: Reduces WebSocket message overhead by batching small packets (configurable timing and size thresholds)
- **Connection Pooling**: Efficient reuse of worker connections with configurable maximum concurrent connections
- **Asynchronous I/O**: Built on Python's `asyncio` with optional `uvloop` integration for maximum performance
- **Buffer Management**: Intelligent buffering with configurable limits to prevent memory exhaustion

### Reliability & Monitoring
- **Real-time Health Checks**: Continuous monitoring of worker health through dedicated ping/pong mechanism
- **EWMA-Based Metrics**: Exponentially Weighted Moving Average for success rates and latency, providing smooth, responsive scoring
- **Destination Cache**: Speeds up repeated connections by remembering which worker last handled a destination
- **Comprehensive Dashboards**: Both terminal (with rich colors) and web-based dashboards with real-time metrics
- **JSON API**: Programmatic access to all metrics for custom monitoring solutions

### Security
- **HMAC Authentication**: Secure challenge-response authentication using HMAC-SHA256
- **Replay Attack Protection**: Nonce-based system with configurable time windows
- **Timestamp Validation**: Prevents clock-skew attacks and ensures freshness
- **Configurable Secrets**: Environment variable-based password management

---

## 🏗 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT APPLICATIONS                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ Browser  │  │   curl   │  │  wget    │  │  git     │  │  Docker  │   ...     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘          │
│       │             │             │             │             │                  │
│       └─────────────┴─────────────┴─────────────┴─────────────┘                  │
│                                     │                                              │
│                            ┌────────▼────────┐                                     │
│                            │  HTTP/SOCKS5    │                                     │
│                            │   Proxy Server  │                                     │
│                            │  (127.0.0.1:1080)│                                    │
│                            └────────┬────────┘                                     │
│                                     │                                              │
│                   ┌─────────────────┼─────────────────┐                            │
│                   │                 │                 │                            │
│            ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐                   │
│            │  Worker 1   │   │  Worker 2   │   │  Worker N   │                   │
│            │  Scoring    │   │  Scoring    │   │  Scoring    │                   │
│            │  Health     │   │  Health     │   │  Health     │                   │
│            │  Checks     │   │  Checks     │   │  Checks     │                   │
│            └──────┬──────┘   └──────┬──────┘   └──────┬──────┘                   │
│                   │                 │                 │                            │
│                   └─────────────────┼─────────────────┘                            │
│                                     │                                              │
│                          ┌──────────▼──────────┐                                   │
│                          │  Cloudflare Edge    │                                   │
│                          │  Network (Workers)  │                                   │
│                          └──────────┬──────────┘                                   │
│                                     │                                              │
└─────────────────────────────────────┼──────────────────────────────────────────────┘
                                      │
                         ┌────────────┼────────────┐
                         │            │            │
                  ┌──────▼──────┐  ┌──▼──┐   ┌─────▼─────┐
                  │  Target     │  │Target│   │  Target   │
                  │  Server 1   │  │Srv 2 │   │  Server 3 │
                  │  (example)  │  │      │   │           │
                  └─────────────┘  └──────┘   └───────────┘
```

---

## 🔧 How It Works

### 1. Handshake & Authentication Flow

The connection establishment process is a multi-step handshake designed for security and reliability:

```
Client (Python)                    Cloudflare Worker
     │                                    │
     │  1. Establish WebSocket            │
     │───────────────────────────────────►│
     │                                    │
     │  2. Send Handshake (JSON)          │
     │  {                                 │
     │    "hostname": "example.com",      │
     │    "port": 443,                    │
     │    "auth": {                       │
     │      "ts": "1712345678",          │
     │      "nonce": "a1b2c3d4...",      │
     │      "sig": "e5a4d3c2..."         │
     │    }                               │
     │  }                                 │
     │───────────────────────────────────►│
     │                                    │
     │                   3. Verify Auth   │
     │                   - Check timestamp│
     │                   - Validate HMAC  │
     │                   - Verify nonce   │
     │                                    │
     │  4. Response (Status)              │
     │  {"status": "connected"}          │
     │◄───────────────────────────────────│
     │                                    │
     │  5. Raw TCP Data Relay Starts      │
     │  (Binary WebSocket Frames)         │
     │◄═══════════════════════════════════►│
     │                                    │
```

**Detailed Steps:**

1. **WebSocket Upgrade**: The client initiates a WebSocket connection to the worker's URL (e.g., `wss://your-worker.workers.dev`).
2. **Handshake Message**: The client constructs a JSON payload containing:
   - `hostname`: Target server address
   - `port`: Target server port
   - `auth`: Authentication object with timestamp, nonce, and HMAC signature
3. **Server Verification**: The worker performs three critical checks:
   - **Timestamp Validity**: Ensures the `ts` is within `AUTH_WINDOW` (default 30 seconds) of current time
   - **HMAC Verification**: Recomputes the signature using the shared secret
   - **Nonce Uniqueness**: Ensures the nonce hasn't been used in the current time window (prevents replay attacks)
4. **Connection Establishment**: If all checks pass, the worker connects to the target server and returns a success response
5. **Data Relay**: From this point forward, all WebSocket messages are raw TCP data, efficiently relayed between client and target

### 2. Data Relay & Coalescing

The data relay mechanism is optimized for both throughput and latency:

**Client → Worker Direction:**
- Data from applications is read from the local TCP socket
- The client buffers data and sends it in configurable chunks (up to `WRITE_BUFFER_MAX`)
- WebSocket frames are sent with minimal overhead

**Worker → Client Direction:**
- The worker reads data from the target server
- **Coalescing Algorithm**:
  1. Data is added to an internal buffer
  2. A timer (`COALESCE_MS`, default 4ms) is started
  3. If the buffer reaches `COALESCE_MAX` (default 65KB), data is sent immediately
  4. If the timer expires, any buffered data is sent
  5. This reduces WebSocket frame overhead while maintaining low latency

**Benefits of Coalescing:**
- Reduces WebSocket message count by up to 90% for small packets
- Improves throughput for bulk data transfers
- Maintains low latency for interactive protocols

### 3. HTTP/HTTPS CONNECT Flow

BatProxy handles HTTP CONNECT method (used for HTTPS, WebSockets, and other TLS tunnels):

```
Browser/Client          BatProxy Client          Worker              Target Server
     │                        │                    │                      │
     │ 1. CONNECT example.com:443                 │                      │
     │───────────────────────►│                    │                      │
     │                        │                    │                      │
     │                        │ 2. Select Worker   │                      │
     │                        │ 3. Handshake       │                      │
     │                        │───────────────────►│                      │
     │                        │                    │                      │
     │                        │ 4. Connected       │                      │
     │                        │◄───────────────────│                      │
     │                        │                    │                      │
     │ 5. 200 OK             │                    │                      │
     │◄───────────────────────│                    │                      │
     │                        │                    │                      │
     │ 6. TLS Tunnel Established (Bidirectional Relay)                  │
     │◄══════════════════════►│◄══════════════════►│◄══════════════════►│
```

**Process Flow:**
1. Client sends HTTP CONNECT request to BatProxy
2. BatProxy selects the healthiest worker using the scoring algorithm
3. Worker handshake and authentication complete
4. Worker establishes TCP connection to the target
5. BatProxy responds with 200 Connection Established
6. All subsequent data is transparently relayed through the tunnel

### 4. SOCKS5 Flow

The SOCKS5 protocol implementation supports all standard features:

```
Browser/Client          BatProxy Client          Worker              Target Server
     │                        │                    │                      │
     │ 1. SOCKS5 Handshake    │                    │                      │
     │───────────────────────►│                    │                      │
     │ 2. Auth Method (NoAuth)│                    │                      │
     │◄───────────────────────│                    │                      │
     │                        │                    │                      │
     │ 3. CONNECT Request     │                    │                      │
     │    (host, port)        │                    │                      │
     │───────────────────────►│                    │                      │
     │                        │                    │                      │
     │                        │ 4. Select Worker   │                      │
     │                        │ 5. Handshake       │                      │
     │                        │───────────────────►│                      │
     │                        │                    │                      │
     │                        │ 6. Connected       │                      │
     │                        │◄───────────────────│                      │
     │                        │                    │                      │
     │ 7. SOCKS5 Response (Success)               │                      │
     │◄───────────────────────│                    │                      │
     │                        │                    │                      │
     │ 8. Bidirectional Data Relay                │                      │
     │◄══════════════════════►│◄══════════════════►│◄══════════════════►│
```

**Supported SOCKS5 Features:**
- `CONNECT` command (TCP tunneling)
- Domain name resolution (ATYP=0x03)
- IPv4 and IPv6 addressing
- No authentication (username/password supported via extension)

### 5. Smart Worker Selection Algorithm

The worker selection algorithm uses a sophisticated scoring system to choose the optimal worker:

```python
def calculate_score(worker_state):
    # Base success rate (EWMA)
    score = worker_state.ewma_success
    
    # RTT penalty (lower is better)
    rtt_penalty = (worker_state.ewma_rtt or 200.0) / 10.0
    score -= rtt_penalty
    
    # Slow streak penalty
    if worker_state.slow_streak >= 3:
        score -= 20.0
    
    # Availability penalty
    if worker_state.state == "open" and cooldown_remaining > 0:
        return -1.0  # Exclude from selection
    
    # Load penalty
    if worker_state.active >= max_connections:
        score -= 50.0
    
    return score
```

**Scoring Factors:**

| Factor | Weight | Description |
|--------|--------|-------------|
| Success Rate (EWMA) | High | Weighted average of recent successes, α=0.35 |
| RTT (EWMA) | Medium | Weighted average of connection latency, α=0.35 |
| Slow Streak | Medium | Consecutive slow connections (>600ms) |
| Active Connections | Low | Current load on the worker |
| Cooldown Status | Critical | Workers in cooldown are excluded |

**Selection Process:**
1. Filter out workers in cooldown or at capacity
2. Calculate score for each remaining worker
3. Sort by score (highest first)
4. Select the top-scoring worker for the request

### 6. Circuit Breaker Algorithm

The circuit breaker pattern prevents cascading failures:

```
Worker States:
┌─────────┐
│ CLOSED  │ ← Normal operation (successful requests)
└────┬────┘
     │ Consecutive failures >= 3
     ▼
┌─────────┐
│ HALF-OPEN│ ← Probationary state (limited retries)
└────┬────┘
     │ Additional failure
     ▼
┌─────────┐
│  OPEN   │ ← Cooldown period (excluded from selection)
└────┬────┘
     │ Cooldown expires / Health check success
     ▼
┌─────────┐
│ HALF-OPEN│ ← Retry with one request
└────┬────┘
     │ Success
     ▼
┌─────────┐
│ CLOSED  │ ← Full recovery
└─────────┘
```

**Transition Rules:**
- **CLOSED → HALF-OPEN**: After 3 consecutive failures
- **HALF-OPEN → OPEN**: After another failure (cooldown begins)
- **OPEN → HALF-OPEN**: Cooldown timer expires or health check succeeds
- **HALF-OPEN → CLOSED**: A single successful request

**Cooldown Calculation:**
```python
cooldown = min(
    COOLDOWN_BASE * (2 ** (consec_failures - HALF_OPEN_AFTER_FAILS)),
    COOLDOWN_MAX
)
```
- Starts at 5 seconds
- Doubles with each consecutive failure
- Caps at 120 seconds

### 7. Health Check System

The background health check system continuously monitors worker health:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Health Check Loop                          │
│                   (Every HEALTH_CHECK_INTERVAL)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. For each worker in "open" state:                           │
│     ├── Send "ping" command with HMAC authentication          │
│     ├── Wait for "pong" response (timeout: CONNECT_TIMEOUT)   │
│     ├── If successful:                                         │
│     │   ├── Update EWMA RTT                                   │
│     │   ├── Transition state: OPEN → HALF-OPEN               │
│     │   └── Decrement consecutive failure counter             │
│     └── If failed:                                             │
│         └── Keep in OPEN state (extend cooldown)              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Health Check Features:**
- Only targets workers in the OPEN state (reduces overhead)
- Uses the same authentication mechanism as regular requests
- Updates latency metrics even when no traffic is flowing
- Gradually recovers workers without risk of overwhelming them

### 8. Destination Cache

The destination cache improves performance for repeated connections to the same target:

```python
dest_cache = {
    "example.com:443": {
        "url": "wss://worker1.workers.dev",
        "ts": 1712345678.0
    },
    "api.github.com:443": {
        "url": "wss://worker2.workers.dev",
        "ts": 1712345680.0
    }
}
```

**Cache Benefits:**
- Reduces worker selection overhead for repeated connections
- Prefers the last successful worker for a destination
- Automatic cleanup of expired entries (TTL: 600 seconds)
- Size-limited to prevent memory leaks

### 9. Retry Logic & Idempotency

BatProxy implements intelligent retry logic for improved reliability:

**Retry Conditions:**
- Network errors (connection refused, timeout, etc.)
- Worker failures (handshake failure, authentication error)
- Circuit breaker activation

**Idempotent Methods (GET, HEAD, OPTIONS, TRACE):**
- Automatically retry on failure
- Each retry uses a different worker (if available)
- Up to `MAX_ATTEMPTS_PER_REQUEST` retries

**Non-Idempotent Methods (POST, PUT, DELETE, PATCH):**
- No automatic retry (to prevent duplicate operations)
- User must handle failures manually
- Single attempt per request

**Retry Flow:**
```
1. Attempt request with best worker
2. If failed:
   ├── Mark worker as failed
   ├── Add worker to exclusion set
   ├── If method is idempotent and attempts < MAX_ATTEMPTS:
   │   ├── Select next best worker
   │   └── Go to step 1
   └── Else:
       └── Return error to client
```

---

## 🔬 Technical Deep Dive

### Cloudflare Worker Internals

The Cloudflare Worker component is written in JavaScript and runs on Cloudflare's V8-based edge runtime.

**Core Functions:**

1. **`fetch(request, env, ctx)`**: The main entry point for all HTTP requests
   - Handles WebSocket upgrade requests
   - Processes ping/pong commands
   - Manages the entire connection lifecycle

2. **`verifyAuth(passwd, subject, auth)`**: Authentication verification
   - Implements HMAC-SHA256 with the shared secret
   - Validates timestamp within `AUTH_WINDOW`
   - Uses `crypto.subtle` for secure HMAC computation

3. **`claimNonce(nonce, now)`**: Nonce management
   - Stores used nonces with expiry timestamps
   - Prevents replay attacks within the current time window
   - Automatically prunes expired nonces

4. **Data Coalescing**: The worker implements intelligent buffering
   - `queueSend()`: Adds data to send buffer
   - `flushSend()`: Sends all buffered data as a single WebSocket frame
   - Timer-based and size-based flushing

**Key Implementation Details:**

```javascript
// WebSocket Pair Creation
const { 0: client, 1: server } = new WebSocketPair();

// TCP Connection via Cloudflare's connect API
tcpSocket = connect({ hostname, port });

// Bidirectional Piping
const reader = tcpSocket.readable.getReader();
while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) queueSend(value);
}
```

### Python Client Internals

The Python client is built on `asyncio` with a modular, event-driven architecture.

**Core Components:**

1. **`handle_client(reader, writer)`**: Main entry point for client connections
   - Reads first byte to determine protocol (HTTP vs SOCKS5)
   - Delegates to protocol-specific handlers

2. **`open_tunnel(hostname, port, exclude)`**: Worker connection manager
   - Selects optimal worker using scoring algorithm
   - Performs handshake and authentication
   - Returns WebSocket connection and worker reference

3. **`relay(reader, writer, ws, preloaded)`**: Data relay coordinator
   - Creates two concurrent tasks for bidirectional transfer
   - Handles graceful shutdown and cleanup

4. **`pipe_local_to_ws(reader, ws)`**: Local → Worker pipeline
   - Reads from local TCP socket
   - Buffers data to reduce WebSocket writes
   - Flushes on timeout or buffer size threshold

5. **`pipe_ws_to_local(ws, writer, preloaded)`**: Worker → Local pipeline
   - Reads from WebSocket
   - Writes to local TCP socket
   - Handles initial data (preloaded response)

**Event Loop Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│                      Asyncio Event Loop                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────┐ │
│  │  HTTP Handler  │  │ SOCKS5 Handler │  │ HTTP Handler │ │
│  └───────┬────────┘  └───────┬────────┘  └──────┬───────┘ │
│          │                   │                   │          │
│          └───────────────────┼───────────────────┘          │
│                              │                              │
│                  ┌───────────▼────────────┐                 │
│                  │  Tunnel Manager        │                 │
│                  │  - Worker Selection    │                 │
│                  │  - Connection Pool    │                 │
│                  │  - Health Checks      │                 │
│                  └───────────┬────────────┘                 │
│                              │                              │
│          ┌───────────────────┼───────────────────┐          │
│          │                   │                   │          │
│  ┌───────▼───────┐  ┌───────▼───────┐  ┌───────▼──────┐ │
│  │  Worker 1     │  │  Worker 2     │  │  Worker N    │ │
│  │  WebSocket    │  │  WebSocket    │  │  WebSocket   │ │
│  └───────────────┘  └───────────────┘  └──────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Authentication Mechanism (HMAC + Nonce + Timestamp)

The authentication system is designed to be secure against various attack vectors:

**Authentication Token Generation (Client Side):**
```python
def make_token(password, subject):
    # Generate timestamp (Unix epoch)
    ts = str(int(time.time()))
    
    # Create random nonce (8 bytes hex)
    nonce = secrets.token_hex(8)
    
    # Construct message: subject:timestamp:nonce
    msg = f"{subject}:{ts}:{nonce}".encode()
    
    # Compute HMAC-SHA256
    sig = hmac.new(password.encode(), msg, hashlib.sha256).hexdigest()
    
    return {"ts": ts, "nonce": nonce, "sig": sig}
```

**Verification Process (Worker Side):**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Authentication Verification                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Input: password, subject, auth_token                          │
│                                                                 │
│  1. Extract: ts, nonce, sig from auth_token                   │
│                                                                 │
│  2. Timestamp Validation:                                      │
│     └── current_time - ts <= AUTH_WINDOW (30s)               │
│                                                                 │
│  3. HMAC Recalculation:                                        │
│     └── expected = HMAC-SHA256(password, subject:ts:nonce)   │
│                                                                 │
│  4. Signature Match:                                           │
│     └── constant_time_compare(expected, sig)                  │
│                                                                 │
│  5. Nonce Uniqueness:                                          │
│     └── claimNonce(nonce, current_time)                       │
│         ├── Check if nonce exists in seenNonces map           │
│         ├── If not, add with expiry (current_time + window)   │
│         └── If exists, reject (replay attack)                 │
│                                                                 │
│  6. All checks passed → Authentication successful              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Security Properties:**
- **Replay Attack Protection**: Nonce ensures each authentication token can only be used once
- **Clock Skew Tolerance**: 30-second window allows for slight time differences
- **Integrity**: HMAC ensures token hasn't been tampered with
- **Confidentiality**: No sensitive data transmitted in plaintext

### Performance Optimizations

BatProxy implements several optimizations for high performance:

**1. Data Coalescing:**
- Reduces WebSocket frame overhead by batching small packets
- Configurable time (4ms) and size (65KB) thresholds
- Can reduce message count by up to 90% for interactive traffic

**2. EWMA Metrics:**
- Smooths out anomalies in latency and success rate measurements
- Responsive to changes while filtering noise
- α = 0.35 for both success rate and RTT

**3. Connection Pooling:**
- Reuses worker connections for multiple requests
- Limits active connections per worker to prevent overload
- Graceful handling of connection limits

**4. Asynchronous I/O:**
- Non-blocking operations throughout
- `asyncio` provides high concurrency with low overhead
- Optional `uvloop` for even better performance

**5. Buffer Management:**
- Configurable buffer limits prevent memory exhaustion
- Automatic flushing on timer or size threshold
- Proper handling of backpressure

### Security Considerations

**1. Worker Security:**
- All traffic is end-to-end encrypted between client and worker
- WebSocket connections use WSS (TLS) by default
- No persistent storage of sensitive data

**2. Authentication Security:**
- HMAC-SHA256 provides strong integrity protection
- Nonce mechanism prevents replay attacks
- Timestamp validation prevents expired token usage

**3. Data Protection:**
- All data is transmitted over TLS
- No logging of sensitive information (by default)
- Configurable to disable logging if needed

**4. Operational Security:**
- Cloudflare Workers run in isolated V8 isolates
- No shared memory between workers
- Automatic scaling and load distribution

**5. Recommended Practices:**
- Use strong, unique passwords (15+ characters)
- Rotate passwords periodically
- Monitor worker activity for anomalies
- Use dedicated Cloudflare accounts per deployment
- Consider additional encryption layers for sensitive data

---

## 📊 Monitoring & Dashboards

### Terminal Dashboard

The terminal dashboard provides real-time monitoring with colored output:

```
🦇 Bat Proxy listening on 127.0.0.1:1080 (engine: uvloop, workers: 3, verbose: False)
  curl (HTTP)   : curl -x http://127.0.0.1:1080 https://example.com
  curl (SOCKS5) : curl -x socks5h://127.0.0.1:1080 https://example.com
  Browser       : set proxy to 127.0.0.1:1080 (HTTP or SOCKS5, all protocols)
  (tip: pip install rich  -> colored live dashboard)

╭────────────────────────────────────────────────────────────────────╮
│ 🦇 Bat Proxy                                                     │
├────────────────────────────────────────────────────────────────────┤
│ Active │ Total │ OK    │ Failed                                  │
│ 12     │ 1547  │ 1532  │ 15                                      │
├────────────────────────────────────────────────────────────────────┤
│ Workers                                                          │
├──────────┬──────────┬──────────┬──────────┬──────────┬──────────┤
│ Worker   │ Status   │ Conns    │ RTT      │ Score    │ OK/Fail  │
├──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
│ worker1  │ closed   │ 8        │ 124ms    │ 85       │ 512/5    │
│ worker2  │ half-open│ 2        │ 189ms    │ 72       │ 421/8    │
│ worker3  │ open 45s │ 2        │ 345ms    │ -1       │ 199/2    │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

**Dashboard Components:**
- **Summary Row**: Active connections, total requests, success/failure counts
- **Worker Table**: Per-worker status, connections, RTT, score, and statistics
- **Color Coding**: Green (success), Yellow (warning), Red (failure)

### Web Dashboard

The HTML dashboard provides a clean, browser-based interface:

**Features:**
- **Auto-refresh**: Updates every second
- **Responsive Design**: Works on desktop and mobile
- **Visual Status**: Color-coded badges for worker states
- **Progress Bars**: Visual representation of worker scores
- **JSON API**: Programmatic access at `/api/stats`

**Endpoint Structure:**
```json
{
  "active": 12,
  "total": 1547,
  "ok": 1532,
  "fail": 15,
  "workers": [
    {
      "url": "worker1",
      "status": "closed",
      "cooldown": 0,
      "active": 8,
      "rtt": 124.5,
      "score": 85.3,
      "ok": 512,
      "fail": 5
    }
  ]
}
```

---

## 📋 Prerequisites & Requirements

### System Requirements
- **Python**: 3.8 or higher
- **Operating System**: Linux, macOS, Windows (with WSL2 recommended)
- **Network**: Internet connection with WebSocket support
- **Memory**: Minimum 256MB RAM (1GB+ recommended)
- **Storage**: 100MB free space

### Cloudflare Requirements
- **Account**: Free Cloudflare account (Workers free tier included)
- **Workers**: Worker limit depends on Cloudflare plan
- **Email**: For enhanced privacy, use disposable email services

### Dependencies
```
websockets>=10.0.0
rich>=13.0.0        # Optional, for better dashboard
uvloop>=0.17.0      # Optional, for better performance
```

---

## 🚀 Installation Guide

### Cloudflare Deployment

**Step 1: Create Cloudflare Account**
1. Visit [Cloudflare Workers](https://workers.cloudflare.com/)
2. Sign up with your email (consider using [ProtonMail](https://proton.me/mail) or similar service)
3. Verify your email address

**Step 2: Create Worker**
1. From the Cloudflare dashboard, navigate to **"Compute"** → **"Workers & Pages"**
2. Click **"Create application"**
3. If you don't have a domain:
   - Click **"Start with hello world"**
   - Click **"Deploy"**
   - A subdomain will be auto-generated (e.g., `your-name.workers.dev`)
4. If you have a domain:
   - Select your domain from the list
   - Click **"Create Worker"**

**Step 3: Deploy Worker Code**
1. Click **"Edit Code"** (top right corner)
2. Delete all default code
3. Copy the code from [worker/worker.js](https://github.com/batmanpriv/BatProxy/blob/main/worker/worker.js)
4. Paste the code into the editor
5. Click **"Save and Deploy"**

### Setting PASSWD Secret

**Option 1: Using Cloudflare Dashboard**
1. Go to your Worker's settings
2. Navigate to **"Variables"** or **"Environment Variables"**
3. Add a new variable:
   - Name: `PASSWD`
   - Value: Your chosen password (e.g., `MySecureP@ssw0rd2024!`)
4. Click **"Save"**

**Option 2: Using Wrangler CLI**
```bash
# Install Wrangler
npm install -g wrangler

# Login
wrangler login

# Set secret
wrangler secret put PASSWD
# (Enter your password when prompted)

# Deploy
wrangler deploy
```

### Python Installation

**1. Install Python 3.8+**
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-pip

# macOS (using Homebrew)
brew install python3

# Windows (using Chocolatey)
choco install python3
```

**2. Clone the Repository**
```bash
git clone https://github.com/batmanpriv/BatProxy.git
cd BatProxy
```

**3. Install Dependencies**
```bash
# Install core dependencies
pip install websockets

# Optional: Install for better dashboard
pip install rich

# Optional: Install for better performance
pip install uvloop
```

**4. Verify Installation**
```bash
python batproxy.py --version
# Should output: BatProxy v2.0
```

---

## ⚙️ Configuration

### Client Configuration

The client is configured via `configx.py` or environment variables.

**Worker Configuration:**
```python
# configx.py
WORKERS = [
    {"url": "wss://worker1.workers.dev", "password": "your_password"},
    {"url": "wss://worker2.workers.dev", "password": "your_password"},
    # Add more workers for redundancy
]
```

**Performance Settings:**
```python
# Connection and timeout settings
CONNECT_TIMEOUT = 6          # Seconds to wait for connection
COOLDOWN_BASE = 5            # Initial cooldown in seconds
COOLDOWN_MAX = 120           # Maximum cooldown in seconds
MAX_ATTEMPTS_PER_REQUEST = 4 # Maximum retry attempts

# Buffer and coalescing settings
WRITE_BUFFER_MAX = 65536     # 64KB buffer
WRITE_BUFFER_DELAY = 0.004   # 4ms delay

# Load balancing settings
MAX_CONN_PER_WORKER = 150    # Maximum concurrent connections
HALF_OPEN_AFTER_FAILS = 3    # Failures before circuit opens

# Cache settings
DEST_CACHE_TTL = 600         # 10 minutes cache TTL
DEST_CACHE_CLEANUP_INTERVAL = 60  # 1 minute cleanup interval

# EWMA settings (success rate)
ALPHA_SUCCESS = 0.35         # Smoothing factor for success rate
ALPHA_RTT = 0.35             # Smoothing factor for RTT

# Performance thresholds
SLOW_RTT_MS = 600            # Considered "slow" in milliseconds
SLOW_PENALTY = 20.0          # Score penalty for slow workers
```

### Worker Configuration

**Environment Variables:**
```javascript
// worker.js - can be set via Cloudflare dashboard
const DEFAULT_PASSWORD = '123456';        // Fallback if PASSWD not set
const AUTH_WINDOW = 30;                  // Seconds
const COALESCE_MS = 4;                   // Milliseconds
const COALESCE_MAX = 65536;              // Bytes
const MAX_HANDSHAKE_BYTES = 2048;        // Bytes
const PING_TARGET_HOST = '1.1.1.1';      // Health check target
const PING_TARGET_PORT = 443;            // Health check port
const MAX_NONCES = 5000;                 // Maximum stored nonces
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PASSWD` | Authentication password | `123456` |
| `AUTH_WINDOW` | Authentication time window (seconds) | `30` |
| `COALESCE_MS` | Data coalescing delay (milliseconds) | `4` |
| `COALESCE_MAX` | Data coalescing size limit (bytes) | `65536` |
| `MAX_HANDSHAKE_BYTES` | Maximum handshake size (bytes) | `2048` |
| `PING_TARGET_HOST` | Host for health checks | `1.1.1.1` |
| `PING_TARGET_PORT` | Port for health checks | `443` |
| `MAX_NONCES` | Maximum stored nonces | `5000` |

### Command Line Arguments

```bash
python batproxy.py [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| `-v, --verbose` | Enable verbose logging | False |
| `--host HOST` | Local proxy host | `127.0.0.1` |
| `--port PORT` | Local proxy port | `1080` |
| `--dashboard-host HOST` | Dashboard host | `127.0.0.1` |
| `--dashboard-port PORT` | Dashboard port | `8088` |
| `--no-web-dashboard` | Disable web dashboard | False |

**Examples:**
```bash
# Default configuration
python batproxy.py

# Custom proxy port with verbose logging
python batproxy.py --port 8080 -v

# Run without web dashboard
python batproxy.py --no-web-dashboard

# Custom dashboard port
python batproxy.py --dashboard-port 9090
```

---

## 💡 Usage Examples

### Browser Configuration

**Firefox:**
1. Settings → Network Settings → Manual proxy configuration
2. HTTP Proxy: `127.0.0.1` Port: `1080`
3. Also use for: HTTPS, SOCKS5
4. Check "Proxy DNS when using SOCKS v5"

**Chrome/Chromium:**
```bash
# Command line
chrome --proxy-server="http://127.0.0.1:1080"

# Or via Settings → System → Open your computer's proxy settings
```

**System Proxy (Ubuntu):**
```bash
# Set HTTP/HTTPS proxy
export http_proxy="http://127.0.0.1:1080"
export https_proxy="http://127.0.0.1:1080"

# Set SOCKS5 proxy
export all_proxy="socks5://127.0.0.1:1080"
```

### curl Examples

**HTTP/HTTPS Proxy:**
```bash
# Basic HTTP GET
curl -x http://127.0.0.1:1080 https://example.com

# HTTP GET with headers
curl -x http://127.0.0.1:1080 -H "User-Agent: BatProxy" https://api.example.com/data

# POST request with data
curl -x http://127.0.0.1:1080 -X POST -d '{"key":"value"}' https://httpbin.org/post

# Download file through proxy
curl -x http://127.0.0.1:1080 -O https://example.com/file.zip
```

**SOCKS5 Proxy:**
```bash
# Basic SOCKS5
curl -x socks5h://127.0.0.1:1080 https://example.com

# SOCKS5 with authentication (if enabled)
curl -x socks5h://user:pass@127.0.0.1:1080 https://example.com

# SOCKS5 for all protocols
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

### Multi-Worker Configuration

For maximum reliability and performance, configure multiple workers:

```python
# config.py
WORKERS = [
    # Primary worker (US region)
    {"url": "wss://us-worker.workers.dev", "password": "secure_password"},
    
    # Backup worker (EU region)
    {"url": "wss://eu-worker.workers.dev", "password": "secure_password"},
    
    # Additional worker (Asia region)
    {"url": "wss://asia-worker.workers.dev", "password": "secure_password"},
    
    # Load balancing across multiple regions
    {"url": "wss://worker1.example.com", "password": "secure_password"},
    {"url": "wss://worker2.example.com", "password": "secure_password"},
    {"url": "wss://worker3.example.com", "password": "secure_password"},
]
```

### Recommended Settings

**For Maximum Reliability:**
```python
MAX_ATTEMPTS_PER_REQUEST = 5
CONNECT_TIMEOUT = 8
HALF_OPEN_AFTER_FAILS = 2
HEALTH_CHECK_INTERVAL = 15
```

**For Maximum Performance:**
```python
WRITE_BUFFER_MAX = 131072   # 128KB
WRITE_BUFFER_DELAY = 0.002  # 2ms
MAX_CONN_PER_WORKER = 300
COALESCE_MS = 2
COALESCE_MAX = 131072
```

**For Low Latency:**
```python
WRITE_BUFFER_DELAY = 0.001  # 1ms
WRITE_BUFFER_MAX = 8192     # 8KB
COALESCE_MS = 1
COALESCE_MAX = 8192
```

---

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

**1. Connection Timeout**
```
Error: ConnectionError: all workers failed
```
**Solutions:**
- Check your internet connection
- Verify worker URLs are correct
- Increase `CONNECT_TIMEOUT` in config
- Check if Cloudflare Workers are accessible

**2. Authentication Failure**
```
Error: invalid signature
```
**Solutions:**
- Verify password matches between client and worker
- Check system time synchronization (NTP)
- Ensure `PASSWD` environment variable is set correctly
- Restart worker after password change

**3. WebSocket Connection Failed**
```
Error: WebSocket connection failed
```
**Solutions:**
- Check if worker is running (visit URL in browser)
- Verify WebSocket support (check for `wss://` protocol)
- Check firewall settings
- Ensure Cloudflare Worker is not rate-limited

**4. High Latency**
```
Warning: Slow RTT detected
```
**Solutions:**
- Check network quality to Cloudflare edge
- Try different workers (different regions)
- Adjust `SLOW_RTT_MS` threshold
- Consider using `uvloop` for better performance

**5. Memory Issues**
```
MemoryError: Unable to allocate buffer
```
**Solutions:**
- Reduce `WRITE_BUFFER_MAX`
- Reduce `MAX_CONN_PER_WORKER`
- Increase system memory limits
- Restart the client periodically

### Debugging Techniques

**Enable Verbose Logging:**
```bash
python batproxy.py -v
```

**Check Worker Status:**
```bash
# Access dashboard
curl http://127.0.0.1:8088/api/stats
```

**Test Worker Connectivity:**
```python
# Simple connectivity test
python -c "
import websockets
import asyncio
async def test():
    try:
        ws = await websockets.connect('wss://your-worker.workers.dev')
        print('Connection successful')
        await ws.close()
    except Exception as e:
        print(f'Connection failed: {e}')
asyncio.run(test())
"
```

**Monitor Logs:**
```bash
# On Linux/macOS
tail -f /var/log/batproxy.log

# On Windows (PowerShell)
Get-Content -Path C:\batproxy\log.txt -Wait
```

---

## ❓ Frequently Asked Questions

**Q: What is the maximum throughput?**
A: The system can handle 1000+ concurrent connections with proper configuration. Throughput is typically limited by your network connection and Cloudflare's bandwidth limits.

**Q: Can I use it for torrents or P2P?**
A: While technically possible, we recommend against it. Cloudflare Workers have usage limits, and P2P traffic may violate their terms of service.

**Q: Is it secure for banking/private data?**
A: Yes, all traffic is encrypted via WebSocket (WSS) and the authentication system is secure. However, we recommend using additional encryption (VPN, HTTPS) for sensitive data.

**Q: How much does it cost?**
A: Cloudflare Workers has a free tier with limited usage. Paid plans start at $5/month and offer higher limits.

**Q: Can I run multiple clients?**
A: Yes, you can run multiple client instances on different ports, or share the same client across multiple applications.

**Q: What happens if all workers fail?**
A: The client will return a 502 Bad Gateway response to the client application. All workers will go into cooldown and recovery.

**Q: Does it support IPv6?**
A: Yes, both the client and Cloudflare Workers support IPv6 addressing.

**Q: How do I update the worker code?**
A: Edit the code in Cloudflare dashboard and click "Save and Deploy". Wait 1-2 minutes for propagation.

---

## 🗺 Roadmap

### Version 2.1 (Q3 2024)
- [ ] UDP support (for DNS over HTTPS, WebRTC)
- [ ] WebSocket compression for better bandwidth
- [ ] Multi-region health checks
- [ ] Improved load balancing algorithms

### Version 2.2 (Q4 2024)
- [ ] Docker containerization
- [ ] Kubernetes support
- [ ] Prometheus metrics integration
- [ ] Grafana dashboards

### Version 3.0 (Q1 2025)
- [ ] WebRTC-based direct connections (P2P)
- [ ] Machine learning-based worker selection
- [ ] Automatic capacity planning
- [ ] Blockchain-based worker registry

---

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### Report Bugs
- Create an issue with detailed steps to reproduce
- Include version information and system details
- Attach logs (with sensitive info redacted)

### Suggest Features
- Create a feature request issue
- Describe the use case and benefits
- Suggest implementation approach if possible

### Contribute Code
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Ensure code style matches the project
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Code Style
- Python: PEP 8 compliance
- JavaScript: ES6+ standards
- Comments: For complex algorithms
- Documentation: For new features

### Testing
```bash
# Run tests
python -m pytest tests/

# Run linting
pylint batproxy.py
```
