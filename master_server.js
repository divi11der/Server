// Tiny in-memory room directory and WebSocket relay for the prototype.
const http = require('node:http');
const net = require('node:net');
const crypto = require('node:crypto');

const port = Number(process.env.PORT || 8765);
const ttlMs = 60_000;
const rooms = new Map();
const activeRooms = new Map();
const WS_OPEN = 1;

function removeRoom(id) {
  rooms.delete(id);
  const room = activeRooms.get(id);
  if (room) {
    room.host?.close(1001, 'room_closed');
    room.client?.close(1001, 'room_closed');
    activeRooms.delete(id);
  }
}

function encodeFrame(opcode, payload) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  let header;
  if (body.length < 126) {
    header = Buffer.from([0x80 | opcode, body.length]);
  } else if (body.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }
  return Buffer.concat([header, body]);
}

function createWebSocket(socket, initialData) {
  const handlers = new Map();
  let buffer = Buffer.alloc(0);
  let fragmentOpcode = 0;
  let fragments = [];
  let closed = false;
  const ws = {
    readyState: WS_OPEN,
    on(event, callback) {
      if (!handlers.has(event)) handlers.set(event, []);
      handlers.get(event).push(callback);
    },
    emit(event, value) {
      for (const callback of handlers.get(event) || []) callback(value);
    },
    send(value) {
      if (ws.readyState === WS_OPEN) socket.write(encodeFrame(1, value));
    },
    close(code = 1000, reason = '') {
      if (ws.readyState !== WS_OPEN) return;
      ws.readyState = 2;
      const text = Buffer.from(String(reason).slice(0, 120));
      const payload = Buffer.alloc(2 + text.length);
      payload.writeUInt16BE(code, 0);
      text.copy(payload, 2);
      socket.end(encodeFrame(8, payload));
    },
  };
  function finish() {
    if (closed) return;
    closed = true;
    ws.readyState = 3;
    ws.emit('close');
  }
  function parseFrames(chunk) {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 2) {
      const finalFrame = (buffer[0] & 0x80) !== 0;
      const opcode = buffer[0] & 0x0f;
      const masked = (buffer[1] & 0x80) !== 0;
      let length = buffer[1] & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (buffer.length < 4) return;
        length = buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (buffer.length < 10) return;
        const wideLength = buffer.readBigUInt64BE(2);
        if (wideLength > 65536n) return ws.close(1009, 'message_too_large');
        length = Number(wideLength);
        offset = 10;
      }
      if (!masked || length > 65536) return ws.close(1002, 'invalid_frame');
      if (buffer.length < offset + 4 + length) return;
      const mask = buffer.subarray(offset, offset + 4);
      offset += 4;
      const payload = Buffer.from(buffer.subarray(offset, offset + length));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      buffer = buffer.subarray(offset + length);
      if (opcode === 8) {
        if (ws.readyState === WS_OPEN) socket.end(encodeFrame(8, payload));
        finish();
        return;
      }
      if (opcode === 9) { socket.write(encodeFrame(10, payload)); continue; }
      if (opcode === 10) continue;
      if (opcode === 1 || opcode === 2) {
        if (fragmentOpcode !== 0) return ws.close(1002, 'nested_fragment');
        fragmentOpcode = opcode;
        fragments = [payload];
      } else if (opcode === 0 && fragmentOpcode !== 0) {
        fragments.push(payload);
      } else {
        return ws.close(1002, 'invalid_opcode');
      }
      if (finalFrame && fragmentOpcode !== 0) {
        ws.emit('message', Buffer.concat(fragments));
        fragmentOpcode = 0;
        fragments = [];
      }
    }
  }
  socket.on('data', parseFrames);
  socket.on('close', finish);
  socket.on('end', finish);
  socket.on('error', finish);
  if (initialData?.length) parseFrames(initialData);
  return ws;
}

function send(res, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  });
  res.end(body);
}

async function readJson(req) {
  let body = '';
  for await (const chunk of req) {
    body += chunk;
    if (body.length > 4096) throw new Error('body_too_large');
  }
  return JSON.parse(body || '{}');
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') return send(res, 204, {});
  const path = new URL(req.url, 'http://localhost').pathname;
  if (req.method === 'GET' && path === '/health') return send(res, 200, { status: 'ok' });
  if (req.method === 'GET' && path === '/api/servers') {
    const now = Date.now();
    for (const [id, room] of rooms) if (now - room.updated > ttlMs) removeRoom(id);
    return send(res, 200, [...rooms.values()].map(({ updated, ...room }) => room));
  }
  if (req.method === 'POST' && (path === '/api/servers/heartbeat' || path === '/api/servers/remove')) {
    let data;
    try { data = await readJson(req); } catch { return send(res, 400, { error: 'invalid_json' }); }
    if (typeof data.id !== 'string' || !/^[a-fA-F0-9]{16,64}$/.test(data.id)) return send(res, 400, { error: 'invalid_id' });
    if (path.endsWith('/remove')) {
      removeRoom(data.id);
      return send(res, 200, { status: 'removed' });
    }
    if (typeof data.name !== 'string' || data.address !== 'relay' || !Number.isInteger(data.port) || data.port < 1 || data.port > 65535 || !Number.isInteger(data.players) || data.players < 1 || data.players > 2) {
      return send(res, 400, { error: 'invalid_room' });
    }
    const address = data.address;
    if (!address) return send(res, 400, { error: 'host_address_unavailable' });
    rooms.set(data.id, {
      id: data.id,
      name: data.name.trim().slice(0, 40) || 'Без названия',
      address,
      local_address: typeof data.local_address === 'string' && net.isIP(data.local_address) === 4 ? data.local_address : '',
      port: data.port,
      players: data.players,
      max_players: 2,
      updated: Date.now(),
    });
    return send(res, 200, { status: 'listed' });
  }
  return send(res, 404, { error: 'not_found' });
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  const key = req.headers['sec-websocket-key'];
  if (url.pathname !== '/ws' || !key || req.headers.upgrade?.toLowerCase() !== 'websocket') return socket.destroy();
  const accept = crypto.createHash('sha1').update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest('base64');
  socket.setNoDelay(true);
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const ws = createWebSocket(socket, head);
  handleRelayConnection(ws, url);
});

function handleRelayConnection(ws, url) {
  const role = url.searchParams.get('role');
  const roomId = url.searchParams.get('room');
  if (!/^[a-fA-F0-9]{16,64}$/.test(roomId || '') || !rooms.has(roomId)) {
    ws.close(1008, 'room_not_found');
    return;
  }
  let room = activeRooms.get(roomId);
  if (!room) {
    room = { host: null, client: null };
    activeRooms.set(roomId, room);
  }
  if (role === 'host') {
    if (room.host && room.host.readyState === WS_OPEN) {
      ws.close(1008, 'host_already_connected');
      return;
    }
    room.host = ws;
    console.log(`Relay host connected for room ${roomId}`);
  } else if (role === 'client') {
    if (!room.host || room.host.readyState !== WS_OPEN) {
      ws.close(1008, 'host_not_connected');
      return;
    }
    if (room.client && room.client.readyState === WS_OPEN) {
      ws.close(1008, 'room_full');
      return;
    }
    room.client = ws;
    ws.send(JSON.stringify({ type: 'welcome', peer_id: 2 }));
    room.host.send(JSON.stringify({ type: 'peer_joined', peer_id: 2 }));
    console.log(`Relay player connected to room ${roomId}`);
  } else {
    ws.close(1008, 'invalid_role');
    return;
  }

  ws.on('message', raw => {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { return; }
    if (role === 'client' && room.host?.readyState === WS_OPEN && message.type === 'input') {
      room.host.send(JSON.stringify({ type: 'input', peer_id: 2, x: Number(message.x) || 0, y: Number(message.y) || 0 }));
    } else if (role === 'client' && room.host?.readyState === WS_OPEN && message.type === 'ping') {
      room.host.send(JSON.stringify({ type: 'ping', timestamp: Number(message.timestamp) || 0 }));
    } else if (role === 'host' && room.client?.readyState === WS_OPEN && (message.type === 'state' || message.type === 'pong')) {
      room.client.send(JSON.stringify(message));
    }
  });
  ws.on('close', () => {
    if (role === 'host' && room.host === ws) {
      room.host = null;
      room.client?.close(1001, 'host_disconnected');
      room.client = null;
    } else if (role === 'client' && room.client === ws) {
      room.client = null;
      if (room.host?.readyState === WS_OPEN) room.host.send(JSON.stringify({ type: 'peer_left', peer_id: 2 }));
    }
    if (!room.host && !room.client) activeRooms.delete(roomId);
  });
}

server.listen(port, '0.0.0.0', () => console.log(`Attention Please directory and relay listening on 0.0.0.0:${port}`));
