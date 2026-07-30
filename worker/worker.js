import { connect } from 'cloudflare:sockets';

const DEFAULT_PASSWORD = '123456'; 
const AUTH_WINDOW = 30;
const COALESCE_MS = 4;
const COALESCE_MAX = 65536;
const MAX_HANDSHAKE_BYTES = 2048;
const PING_TARGET_HOST = '1.1.1.1';
const PING_TARGET_PORT = 443;
const MAX_NONCES = 5000;
const seenNonces = new Map();

function pruneNonces(now) {
  for (const [nonce, expiry] of seenNonces) {
    if (expiry <= now) seenNonces.delete(nonce);
  }
}

function claimNonce(nonce, now) {
  pruneNonces(now);
  if (seenNonces.has(nonce)) return false;
  if (seenNonces.size >= MAX_NONCES) {
    const oldest = seenNonces.keys().next().value;
    seenNonces.delete(oldest);
  }
  seenNonces.set(nonce, now + AUTH_WINDOW);
  return true;
}

async function hmacHex(password, message) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(password),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(message));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function verifyAuth(passwd, subject, auth) {
  if (!auth || !auth.ts || !auth.nonce || !auth.sig) return false;
  const now = Math.floor(Date.now() / 1000);
  const ts = parseInt(auth.ts, 10);
  if (!ts || Math.abs(now - ts) > AUTH_WINDOW) return false;
  const expected = await hmacHex(passwd, `${subject}:${auth.ts}:${auth.nonce}`);
  if (expected !== auth.sig) return false;
  return claimNonce(auth.nonce, now);
}

export default {
  async fetch(request, env, _ctx) {
    const passwd = env.PASSWD || DEFAULT_PASSWORD;

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Bat Proxy relay - websocket only', { status: 404 });
    }

    const { 0: client, 1: server } = new WebSocketPair();
    server.binaryType = 'arraybuffer';
    server.accept();

    let tcpSocket = null;
    let writer = null;
    let closed = false;
    let sendBuf = [];
    let sendLen = 0;
    let sendTimer = null;

    const toUint8Array = (data) => {
      if (data instanceof ArrayBuffer) return new Uint8Array(data);
      if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      if (typeof data === 'string') return new TextEncoder().encode(data);
      return new Uint8Array(data);
    };

    const flushSend = () => {
      if (sendTimer) {
        clearTimeout(sendTimer);
        sendTimer = null;
      }
      if (!sendLen) return;
      const merged = new Uint8Array(sendLen);
      let off = 0;
      for (const c of sendBuf) {
        merged.set(c, off);
        off += c.byteLength;
      }
      sendBuf = [];
      sendLen = 0;
      try {
        server.send(merged.buffer);
      } catch {}
    };

    const queueSend = (chunk) => {
      sendBuf.push(chunk);
      sendLen += chunk.byteLength;
      if (sendLen >= COALESCE_MAX) {
        flushSend();
      } else if (!sendTimer) {
        sendTimer = setTimeout(flushSend, COALESCE_MS);
      }
    };

    const cleanup = async (reason) => {
      if (closed) return;
      closed = true;
      flushSend();
      try {
        await writer?.close();
      } catch {}
      try {
        await tcpSocket?.close();
      } catch {}
      try {
        server.close(1000, reason || 'closed');
      } catch {}
    };

    server.addEventListener('message', async (event) => {
      try {
        const data = toUint8Array(event.data);

        if (!tcpSocket) {
          if (data.byteLength > MAX_HANDSHAKE_BYTES) {
            return cleanup('handshake too large');
          }

          let parsed;
          try {
            parsed = JSON.parse(new TextDecoder().decode(data));
          } catch {
            server.send(JSON.stringify({ status: 'error', message: 'invalid handshake' }));
            return cleanup('bad handshake');
          }

          if (parsed.cmd === 'ping') {
            const ok = await verifyAuth(passwd, 'ping', parsed.auth);
            if (!ok) {
              server.send(JSON.stringify({ cmd: 'pong', status: 'error', message: 'auth failed' }));
              return cleanup('ping auth failed');
            }
            try {
              const probe = connect({ hostname: PING_TARGET_HOST, port: PING_TARGET_PORT });
              await probe.opened;
              await probe.close();
              server.send(JSON.stringify({ cmd: 'pong', status: 'ok' }));
            } catch (err) {
              server.send(JSON.stringify({ cmd: 'pong', status: 'error', message: String(err) }));
            }
            return cleanup('ping done');
          }

          const { hostname, port, auth } = parsed;
          if (!hostname || !port) {
            server.send(JSON.stringify({ status: 'error', message: 'missing fields' }));
            return cleanup('bad target');
          }
          const ok = await verifyAuth(passwd, `${hostname}:${port}`, auth);
          if (!ok) {
            server.send(JSON.stringify({ status: 'error', message: 'invalid signature' }));
            return cleanup('auth failed');
          }

          try {
            tcpSocket = connect({ hostname, port });
            writer = tcpSocket.writable.getWriter();
            const reader = tcpSocket.readable.getReader();

            (async () => {
              try {
                while (true) {
                  const { value, done } = await reader.read();
                  if (done) break;
                  if (value) queueSend(value);
                }
              } catch {
              } finally {
                reader.releaseLock();
                cleanup('target closed');
              }
            })();

            server.send(JSON.stringify({ status: 'connected', hostname, port }));
          } catch (err) {
            server.send(JSON.stringify({ status: 'error', message: String(err) }));
            cleanup('connect failed');
          }
          return;
        }

        if (writer) {
          await writer.write(data);
        }
      } catch (err) {
        cleanup('handler error');
      }
    });

    server.addEventListener('close', () => cleanup('client closed'));
    server.addEventListener('error', () => cleanup('client error'));

    return new Response(null, { status: 101, webSocket: client });
  },
};
