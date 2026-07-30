import argparse
import asyncio
import hashlib
import hmac
import secrets
import socket
import struct
import time
import json
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from configx import WORKERS
import websocket
import signal
import sys

try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    HAVE_RICH = True
except ImportError:
    HAVE_RICH = False

try:
    import uvloop
    HAVE_UVLOOP = True
except ImportError:
    HAVE_UVLOOP = False

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 1080
DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 8088

CONNECT_TIMEOUT = 6
COOLDOWN_BASE = 5
COOLDOWN_MAX = 120
MAX_ATTEMPTS_PER_REQUEST = 4
HEALTH_CHECK_INTERVAL = 30
HALF_OPEN_AFTER_FAILS = 3
WRITE_BUFFER_MAX = 65536
WRITE_BUFFER_DELAY = 0.004
MAX_CONN_PER_WORKER = 150
DEST_CACHE_TTL = 600
DEST_CACHE_CLEANUP_INTERVAL = 60
ALPHA_SUCCESS = 0.35
ALPHA_RTT = 0.35
SLOW_RTT_MS = 600
SLOW_PENALTY = 20.0
RETRYABLE_METHODS = {"GET", "HEAD"}
WS_PING_INTERVAL = 20

WS_EXECUTOR_WORKERS = 512
ws_executor = ThreadPoolExecutor(max_workers=WS_EXECUTOR_WORKERS, thread_name_prefix="ws-io")

console = Console() if HAVE_RICH else None
VERBOSE = False


def setup_signal_handlers():
    def handler(sig, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

async def cleanup(server):
    print("\n🛑 Cleaning up...")
    server.close()
    await server.wait_closed()
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
    await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)
    ws_executor.shutdown(wait=False)
    print("✅ Cleanup complete")

class ConnectionClosedError(Exception):
    pass


class AsyncWS:
    __slots__ = ("_ws", "_loop", "_ping_task", "_closed")

    def __init__(self, raw_ws, loop):
        self._ws = raw_ws
        self._loop = loop
        self._ping_task = None
        self._closed = False

    async def send(self, data):
        if isinstance(data, (bytes, bytearray)):
            payload = bytes(data)
            opcode = websocket.ABNF.OPCODE_BINARY
        else:
            payload = data
            opcode = websocket.ABNF.OPCODE_TEXT
        try:
            await self._loop.run_in_executor(ws_executor, self._ws.send, payload, opcode)
        except (websocket.WebSocketException, OSError) as e:
            raise ConnectionClosedError(str(e)) from e

    async def recv(self):
        try:
            msg = await self._loop.run_in_executor(ws_executor, self._ws.recv)
        except (websocket.WebSocketException, OSError) as e:
            raise ConnectionClosedError(str(e)) from e
        if msg is None or msg == "":
            if self._ws.connected:
                return msg
            raise ConnectionClosedError("connection closed")
        return msg

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self.stop_keepalive()
        try:
            await self._loop.run_in_executor(ws_executor, self._ws.close)
        except Exception:
            pass

    def start_keepalive(self, interval=WS_PING_INTERVAL):
        if interval and self._ping_task is None:
            self._ping_task = asyncio.ensure_future(self._keepalive_loop(interval))

    def stop_keepalive(self):
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()

    async def _keepalive_loop(self, interval):
        try:
            while True:
                await asyncio.sleep(interval)
                await self._loop.run_in_executor(ws_executor, self._ws.ping)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            msg = await self.recv()
        except ConnectionClosedError:
            raise StopAsyncIteration
        return msg


async def ws_connect(url, timeout=CONNECT_TIMEOUT, keepalive=0):
    """Async wrapper around websocket-client's create_connection()."""
    loop = asyncio.get_event_loop()

    def _do_connect():
        return websocket.create_connection(
            url,
            timeout=timeout,
            enable_multithread=True,
        )

    try:
        raw_ws = await asyncio.wait_for(
            loop.run_in_executor(ws_executor, _do_connect),
            timeout=timeout,
        )
    except asyncio.TimeoutError as e:
        raise ConnectionError(f"connect timeout: {url}") from e
    except (websocket.WebSocketException, OSError, ValueError) as e:
        raise ConnectionError(str(e)) from e

    ws = AsyncWS(raw_ws, loop)
    if keepalive:
        ws.start_keepalive(keepalive)
    return ws

def make_token(password, subject):
    ts = str(int(time.time()))
    nonce = secrets.token_hex(8)
    msg = f"{subject}:{ts}:{nonce}".encode()
    sig = hmac.new(password.encode(), msg, hashlib.sha256).hexdigest()
    return {"ts": ts, "nonce": nonce, "sig": sig}


class WorkerState:
    def __init__(self, url, password):
        self.url = url
        self.password = password
        self.ok = 0
        self.fail = 0
        self.consec_fail = 0
        self.ewma_rtt = None
        self.ewma_success = 80.0
        self.slow_streak = 0
        self.active = 0
        self.state = "closed"
        self.cooldown_until = 0.0

    @property
    def score(self):
        if self.state == "open" and time.time() < self.cooldown_until:
            return -1.0
        rtt_penalty = (self.ewma_rtt or 200.0) / 10.0
        slow_penalty = SLOW_PENALTY if self.slow_streak >= 3 else 0.0
        return self.ewma_success - rtt_penalty - slow_penalty


class Stats:
    def __init__(self, workers):
        self.active = 0
        self.total = 0
        self.ok = 0
        self.fail = 0
        self.workers = {w["url"]: WorkerState(w["url"], w["password"]) for w in workers}


stats = Stats(WORKERS)
dest_cache = {}


def worker_short(url):
    return url.split("//", 1)[-1][:32]


def log(msg, style=None, verbose_only=False):
    if verbose_only and not VERBOSE:
        return
    if HAVE_RICH:
        console.print(msg, style=style)
    else:
        print(msg)


def pick_workers_order(exclude=None):
    exclude = exclude or set()
    now = time.time()
    candidates = []
    for w in WORKERS:
        if w["url"] in exclude:
            continue
        st = stats.workers[w["url"]]
        if st.state == "open" and now < st.cooldown_until:
            continue
        candidates.append(w)
    candidates.sort(key=lambda w: (stats.workers[w["url"]].active >= MAX_CONN_PER_WORKER, -stats.workers[w["url"]].score))
    if not candidates:
        candidates = sorted((w for w in WORKERS if w["url"] not in exclude), key=lambda w: stats.workers[w["url"]].cooldown_until)
    return candidates


def mark_success(worker, rtt):
    st = stats.workers[worker["url"]]
    st.ok += 1
    st.consec_fail = 0
    st.state = "closed"
    st.cooldown_until = 0.0
    st.ewma_success = st.ewma_success * (1 - ALPHA_SUCCESS) + 100 * ALPHA_SUCCESS
    st.ewma_rtt = rtt if st.ewma_rtt is None else st.ewma_rtt * (1 - ALPHA_RTT) + rtt * ALPHA_RTT
    st.slow_streak = st.slow_streak + 1 if rtt > SLOW_RTT_MS else 0


def mark_failure(worker):
    st = stats.workers[worker["url"]]
    st.fail += 1
    st.consec_fail += 1
    st.ewma_success = st.ewma_success * (1 - ALPHA_SUCCESS)
    if st.consec_fail >= HALF_OPEN_AFTER_FAILS:
        cooldown = min(COOLDOWN_BASE * (2 ** (st.consec_fail - HALF_OPEN_AFTER_FAILS)), COOLDOWN_MAX)
        st.state = "open"
        st.cooldown_until = time.time() + cooldown
    else:
        st.state = "half_open"


def release_worker(worker):
    if worker is None:
        return
    st = stats.workers.get(worker["url"])
    if st:
        st.active = max(0, st.active - 1)


async def open_tunnel(hostname, port, exclude=None):
    exclude = set(exclude or [])
    key = f"{hostname}:{port}"
    ordered = []
    cached = dest_cache.get(key)
    if cached and cached["url"] not in exclude and (time.time() - cached["ts"]) < DEST_CACHE_TTL:
        cw = next((w for w in WORKERS if w["url"] == cached["url"]), None)
        st = stats.workers.get(cached["url"]) if cw else None
        if cw and st and not (st.state == "open" and time.time() < st.cooldown_until) and st.active < MAX_CONN_PER_WORKER:
            ordered.append(cw)
    for w in pick_workers_order(exclude):
        if w not in ordered:
            ordered.append(w)

    last_err = None
    attempts = 0
    for worker in ordered:
        if attempts >= MAX_ATTEMPTS_PER_REQUEST:
            break
        attempts += 1
        t0 = time.time()
        try:
            ws = await ws_connect(worker["url"], timeout=CONNECT_TIMEOUT, keepalive=WS_PING_INTERVAL)
            token = make_token(worker["password"], key)
            init = json.dumps({"hostname": hostname, "port": port, "auth": token})
            await ws.send(init)
            resp = await asyncio.wait_for(ws.recv(), timeout=CONNECT_TIMEOUT)
            data = json.loads(resp)
            if data.get("status") != "connected":
                raise RuntimeError(data.get("message", "rejected by worker"))

            rtt = (time.time() - t0) * 1000
            mark_success(worker, rtt)
            stats.workers[worker["url"]].active += 1
            dest_cache[key] = {"url": worker["url"], "ts": time.time()}
            log(f"  \u21b3 [cyan]{worker_short(worker['url'])}[/] accepted {hostname}:{port} ({rtt:.0f}ms)",
                verbose_only=True)
            return ws, worker

        except Exception as e:
            last_err = e
            mark_failure(worker)
            log(f"  \u21b3 [yellow]{worker_short(worker['url'])} failed ({e}), trying next\u2026[/]",
                verbose_only=True)
            await asyncio.sleep(0.1)

    raise ConnectionError(f"all workers failed: {last_err}")


async def health_check_loop():
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        for w in WORKERS:
            st = stats.workers[w["url"]]
            if st.state != "open":
                continue
            try:
                ws = await ws_connect(w["url"], timeout=CONNECT_TIMEOUT)
                token = make_token(w["password"], "ping")
                t0 = time.time()
                await ws.send(json.dumps({"cmd": "ping", "auth": token}))
                resp = await asyncio.wait_for(ws.recv(), timeout=CONNECT_TIMEOUT)
                data = json.loads(resp)
                try:
                    await ws.close()
                except Exception:
                    pass
                if data.get("cmd") == "pong" and data.get("status") == "ok":
                    rtt = (time.time() - t0) * 1000
                    st.ewma_rtt = rtt if st.ewma_rtt is None else st.ewma_rtt * 0.5 + rtt * 0.5
                    st.state = "half_open"
                    st.consec_fail = max(0, st.consec_fail - 1)
            except Exception:
                pass


async def dest_cache_cleanup_loop():
    while True:
        await asyncio.sleep(DEST_CACHE_CLEANUP_INTERVAL)
        now = time.time()
        expired = [k for k, v in dest_cache.items() if now - v["ts"] > DEST_CACHE_TTL]
        for k in expired:
            dest_cache.pop(k, None)


def tune_socket(writer):
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass


async def pipe_ws_to_local(ws, writer, preloaded=None):
    buf = bytearray()
    last_flush = time.time()

    async def flush():
        nonlocal buf, last_flush
        if buf:
            writer.write(bytes(buf))
            await writer.drain()
            buf.clear()
        last_flush = time.time()

    try:
        if preloaded is not None:
            buf += preloaded if isinstance(preloaded, bytes) else preloaded.encode()
        async for msg in ws:
            buf += msg if isinstance(msg, bytes) else msg.encode()
            if len(buf) >= WRITE_BUFFER_MAX or (time.time() - last_flush) >= WRITE_BUFFER_DELAY:
                await flush()
        await flush()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def pipe_local_to_ws(reader, ws):
    buf = bytearray()

    async def flush():
        nonlocal buf
        if buf:
            await ws.send(bytes(buf))
            buf.clear()

    try:
        while True:
            try:
                data = await asyncio.wait_for(reader.read(65536), timeout=WRITE_BUFFER_DELAY)
            except asyncio.TimeoutError:
                await flush()
                continue
            if not data:
                break
            buf += data
            if len(buf) >= WRITE_BUFFER_MAX:
                await flush()
        await flush()
    except Exception:
        pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


async def relay(reader, writer, ws, preloaded=None):
    await asyncio.gather(
        pipe_local_to_ws(reader, ws),
        pipe_ws_to_local(ws, writer, preloaded=preloaded),
        return_exceptions=True,
    )


async def handle_http(reader, writer, first_byte):
    first_line = first_byte + await reader.readline()
    if not first_line.strip():
        writer.close()
        return

    header_lines = [first_line]
    while True:
        line = await reader.readline()
        header_lines.append(line)
        if line in (b"\r\n", b""):
            break

    request_line = first_line.decode("latin1").strip()
    parts = request_line.split(" ")
    if len(parts) < 2:
        writer.close()
        return
    method, target = parts[0].upper(), parts[1]

    if method == "CONNECT":
        host, _, port_s = target.partition(":")
        port = int(port_s) if port_s else 443
        proto_desc = f"HTTP CONNECT {host}:{port}"
    else:
        if target.startswith("http://"):
            parsed = urlparse(target)
            host = parsed.hostname
            port = parsed.port or 80
            origin_target = parsed.path or "/"
            if parsed.query:
                origin_target += "?" + parsed.query
        else:
            host, port = None, 80
            for h in header_lines[1:]:
                if h.lower().startswith(b"host:"):
                    hv = h.split(b":", 1)[1].strip().decode()
                    if ":" in hv:
                        host, port_s = hv.split(":", 1)
                        port = int(port_s)
                    else:
                        host = hv
                    break
            origin_target = target
        proto_desc = f"HTTP {method} {host}:{port}{origin_target if host else ''}"

    if not host:
        writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    stats.total += 1

    if method == "CONNECT":
        try:
            ws, worker = await open_tunnel(host, port)
        except Exception as e:
            stats.fail += 1
            log(f"\u2717 {proto_desc}  [bold red]({e})[/]")
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        stats.ok += 1
        log(f"\u2713 {proto_desc}  [dim]via {worker_short(worker['url'])}[/]", verbose_only=not VERBOSE)
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        try:
            await relay(reader, writer, ws)
        finally:
            release_worker(worker)
        return

    new_request_line = f"{method} {origin_target} HTTP/1.1\r\n".encode()
    rest_headers = b"".join(header_lines[1:])
    request_bytes = new_request_line + rest_headers

    exclude = set()
    ws = worker = first = None
    for _ in range(MAX_ATTEMPTS_PER_REQUEST):
        try:
            ws, worker = await open_tunnel(host, port, exclude=exclude)
        except Exception as e:
            last_err = e
            ws = worker = None
            break
        if method in RETRYABLE_METHODS:
            try:
                await ws.send(request_bytes)
                first = await asyncio.wait_for(ws.recv(), timeout=CONNECT_TIMEOUT)
                break
            except Exception as e:
                last_err = e
                mark_failure(worker)
                release_worker(worker)
                exclude.add(worker["url"])
                ws = worker = None
                continue
        else:
            await ws.send(request_bytes)
            break

    if ws is None:
        stats.fail += 1
        log(f"\u2717 {proto_desc}  [bold red](connection failed)[/]")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    stats.ok += 1
    log(f"\u2713 {proto_desc}  [dim]via {worker_short(worker['url'])}[/]", verbose_only=not VERBOSE)
    try:
        await relay(reader, writer, ws, preloaded=first)
    finally:
        release_worker(worker)


SOCKS_FAIL = b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00"
SOCKS_OK = b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00"


async def handle_socks5(reader, writer, first_byte):
    nmethods = (await reader.readexactly(1))[0]
    await reader.readexactly(nmethods)

    writer.write(b"\x05\x00")
    await writer.drain()

    ver_cmd_rsv_atyp = await reader.readexactly(4)
    _, cmd, _, atyp = ver_cmd_rsv_atyp

    if atyp == 0x01:
        host = socket.inet_ntoa(await reader.readexactly(4))
    elif atyp == 0x03:
        length = (await reader.readexactly(1))[0]
        host = (await reader.readexactly(length)).decode()
    elif atyp == 0x04:
        host = socket.inet_ntop(socket.AF_INET6, await reader.readexactly(16))
    else:
        writer.write(SOCKS_FAIL)
        await writer.drain()
        writer.close()
        return

    port = struct.unpack(">H", await reader.readexactly(2))[0]

    if cmd != 0x01:
        writer.write(SOCKS_FAIL)
        await writer.drain()
        writer.close()
        return

    proto_desc = f"SOCKS5 CONNECT {host}:{port}"
    stats.total += 1
    try:
        ws, worker = await open_tunnel(host, port)
    except Exception as e:
        stats.fail += 1
        log(f"\u2717 {proto_desc}  [bold red]({e})[/]")
        writer.write(SOCKS_FAIL)
        await writer.drain()
        writer.close()
        return

    stats.ok += 1
    log(f"\u2713 {proto_desc}  [dim]via {worker_short(worker['url'])}[/]", verbose_only=not VERBOSE)

    writer.write(SOCKS_OK)
    await writer.drain()
    try:
        await relay(reader, writer, ws)
    finally:
        release_worker(worker)


async def handle_client(reader, writer):
    stats.active += 1
    tune_socket(writer)
    try:
        first_byte = await reader.read(1)
        if not first_byte:
            return
        if first_byte == b"\x05":
            await handle_socks5(reader, writer, first_byte)
        else:
            await handle_http(reader, writer, first_byte)
    except Exception:
        pass
    finally:
        stats.active -= 1
        try:
            writer.close()
        except Exception:
            pass


def build_dashboard():
    main = Table(title="\U0001F987 Bat Proxy", show_header=True, header_style="bold magenta")
    main.add_column("Active")
    main.add_column("Total")
    main.add_column("OK", style="green")
    main.add_column("Failed", style="red")
    main.add_row(str(stats.active), str(stats.total), str(stats.ok), str(stats.fail))

    wt = Table(title="Workers")
    wt.add_column("Worker")
    wt.add_column("Status")
    wt.add_column("Conns", justify="right")
    wt.add_column("RTT", justify="right")
    wt.add_column("Score", justify="right")
    wt.add_column("OK", justify="right", style="green")
    wt.add_column("Fail", justify="right", style="red")
    now = time.time()
    for w in WORKERS:
        st = stats.workers[w["url"]]
        if st.state == "open" and st.cooldown_until > now:
            status = f"[red]open {int(st.cooldown_until - now)}s[/]"
        elif st.state == "half_open":
            status = "[yellow]half-open[/]"
        else:
            status = "[green]closed[/]"
        rtt_s = f"{st.ewma_rtt:.0f}ms" if st.ewma_rtt is not None else "-"
        wt.add_row(worker_short(w["url"]), status, str(st.active), rtt_s, f"{st.score:.0f}", str(st.ok), str(st.fail))

    group = Table.grid()
    group.add_row(main)
    group.add_row(wt)
    return group


async def dashboard_loop():
    if not HAVE_RICH:
        while True:
            print(f"\rActive:{stats.active} Total:{stats.total} OK:{stats.ok} Fail:{stats.fail}   ", end="")
            await asyncio.sleep(1)
    with Live(build_dashboard(), console=console, refresh_per_second=4) as live:
        while True:
            live.update(build_dashboard())
            await asyncio.sleep(1)


def dashboard_json():
    now = time.time()
    workers_out = []
    for w in WORKERS:
        st = stats.workers[w["url"]]
        if st.state == "open" and st.cooldown_until > now:
            status = "open"
            cooldown = round(st.cooldown_until - now, 1)
        else:
            status = st.state
            cooldown = 0
        workers_out.append({
            "url": worker_short(w["url"]),
            "status": status,
            "cooldown": cooldown,
            "active": st.active,
            "rtt": round(st.ewma_rtt, 1) if st.ewma_rtt is not None else None,
            "score": round(st.score, 1),
            "ok": st.ok,
            "fail": st.fail,
        })
    return {
        "active": stats.active,
        "total": stats.total,
        "ok": stats.ok,
        "fail": stats.fail,
        "workers": workers_out,
    }


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bat Proxy Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--bg:#0b0d12;--card:#12151d;--border:#1f2430;--text:#e6e9ef;--muted:#8a93a6;--green:#3ddc84;--yellow:#f5c451;--red:#ff5d5d;--accent:#5b8cff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:32px}
h1{font-size:22px;font-weight:600;margin:0 0 4px}
.sub{color:var(--muted);font-size:13px;margin-bottom:28px}
.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:32px}
.stat{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:18px}
.stat .num{font-size:28px;font-weight:700}
.stat .lbl{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em;margin-top:4px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px}
.wcard{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:18px;position:relative;overflow:hidden}
.wcard .name{font-weight:600;font-size:14px;margin-bottom:10px;word-break:break-all}
.badge{display:inline-block;padding:3px 10px;border-radius:999px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.03em}
.badge.closed{background:rgba(61,220,132,.15);color:var(--green)}
.badge.half_open{background:rgba(245,196,81,.15);color:var(--yellow)}
.badge.open{background:rgba(255,93,93,.15);color:var(--red)}
.rows{margin-top:14px;display:flex;flex-direction:column;gap:6px}
.row{display:flex;justify-content:space-between;font-size:13px;color:var(--muted)}
.row b{color:var(--text);font-weight:600}
.scorebar{height:6px;border-radius:3px;background:#1f2430;margin-top:14px;overflow:hidden}
.scorebar div{height:100%;background:linear-gradient(90deg,var(--accent),var(--green))}
</style>
</head>
<body>
<h1>&#129415; Bat Proxy</h1>
<div class="sub">live worker health &amp; throughput</div>
<div class="summary" id="summary"></div>
<div class="grid" id="workers"></div>
<script>
function badge(state){
  return '<span class="badge ' + state + '">' + state.replace('_',' ') + '</span>';
}
async function tick(){
  try{
    const res = await fetch('/api/stats', {cache:'no-store'});
    const data = await res.json();
    document.getElementById('summary').innerHTML = [
      ['Active', data.active],
      ['Total', data.total],
      ['OK', data.ok],
      ['Failed', data.fail]
    ].map(function(p){
      return '<div class="stat"><div class="num">' + p[1] + '</div><div class="lbl">' + p[0] + '</div></div>';
    }).join('');
    document.getElementById('workers').innerHTML = data.workers.map(function(w){
      const scorePct = Math.max(0, Math.min(100, w.score));
      return '<div class="wcard">' +
        '<div class="name">' + w.url + '</div>' +
        badge(w.status) +
        (w.status === 'open' ? '<span style="color:var(--muted);font-size:12px;margin-left:8px">cooldown ' + w.cooldown + 's</span>' : '') +
        '<div class="rows">' +
          '<div class="row"><span>connections</span><b>' + w.active + '</b></div>' +
          '<div class="row"><span>rtt</span><b>' + (w.rtt !== null ? w.rtt + 'ms' : '-') + '</b></div>' +
          '<div class="row"><span>score</span><b>' + w.score + '</b></div>' +
          '<div class="row"><span>ok / fail</span><b>' + w.ok + ' / ' + w.fail + '</b></div>' +
        '</div>' +
        '<div class="scorebar"><div style="width:' + scorePct + '%"></div></div>' +
      '</div>';
    }).join('');
  }catch(e){}
}
tick();
setInterval(tick, 1000);
</script>
</body>
</html>"""


async def handle_dashboard_client(reader, writer):
    try:
        request_line = await reader.readline()
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b""):
                break
        try:
            _, path, _ = request_line.decode().split(" ", 2)
        except Exception:
            path = "/"

        if path.startswith("/api/stats"):
            body = json.dumps(dashboard_json()).encode()
            headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Cache-Control: no-store\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
        else:
            body = DASHBOARD_HTML.encode()
            headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode()

        writer.write(headers + body)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def dashboard_web_server(host, port):
    server = await asyncio.start_server(handle_dashboard_client, host, port)
    log(f"[bold]Web dashboard[/] on [cyan]http://{host}:{port}[/]")
    async with server:
        await server.serve_forever()


def parse_args():
    p = argparse.ArgumentParser(description="Bat Proxy - HTTP + SOCKS5 tunnel via Cloudflare Workers")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--host", default=LOCAL_HOST)
    p.add_argument("--port", type=int, default=LOCAL_PORT)
    p.add_argument("--dashboard-host", default=DASHBOARD_HOST)
    p.add_argument("--dashboard-port", type=int, default=DASHBOARD_PORT)
    p.add_argument("--no-web-dashboard", action="store_true")
    return p.parse_args()


async def main():
    args = parse_args()
    global VERBOSE
    VERBOSE = args.verbose
    
    setup_signal_handlers()
    
    server = await asyncio.start_server(handle_client, args.host, args.port)
    engine = "uvloop" if HAVE_UVLOOP else "asyncio"
    
    banner = (
        f"[bold]\U0001F987 Bat Proxy[/] listening on [cyan]{args.host}:{args.port}[/]  "
        f"(engine: {engine}, workers: {len(WORKERS)}, verbose: {VERBOSE})"
    )
    log(banner)
    log(f"  curl (HTTP)   : curl -x http://{args.host}:{args.port} https://example.com")
    log(f"  curl (SOCKS5) : curl -x socks5h://{args.host}:{args.port} https://example.com")
    log(f"  Browser       : set proxy to {args.host}:{args.port} (HTTP or SOCKS5, all protocols)")
    if not HAVE_RICH:
        log("  (tip: pip install rich  -> colored live dashboard)")
    
    tasks = [server.serve_forever(), dashboard_loop(), health_check_loop(), dest_cache_cleanup_loop()]
    if not args.no_web_dashboard:
        tasks.append(dashboard_web_server(args.dashboard_host, args.dashboard_port))
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        await cleanup(server)
    except Exception as e:
        print(f"Error: {e}")
        await cleanup(server)
    finally:
        print("👋 Goodbye Batman!")

if __name__ == "__main__":
    if HAVE_UVLOOP:
        uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
