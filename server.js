const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const rooms = new Map();
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("DuoChat server running\n");
});

const wss = new WebSocketServer({ server });

function broadcast(room, data, excludeId = null) {
  const clients = rooms.get(room);
  if (!clients) return;
  const json = JSON.stringify(data);
  for (const client of clients) {
    if (client.id !== excludeId && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(json);
    }
  }
}

function roomInfo(roomId) {
  const clients = rooms.get(roomId) || new Set();
  return [...clients].map((c) => ({ id: c.id, username: c.username }));
}

wss.on("connection", (ws) => {
  const clientId = crypto.randomBytes(6).toString("hex");
  let client = { ws, id: clientId, username: null, roomId: null };

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "join") {
      const { username, roomId } = msg;
      if (client.roomId) leaveRoom(client);
      client.username = username.trim().slice(0, 30);
      client.roomId = roomId.trim().toLowerCase().slice(0, 30);
      if (!rooms.has(client.roomId)) rooms.set(client.roomId, new Set());
      rooms.get(client.roomId).add(client);
      ws.send(JSON.stringify({ type: "joined", id: clientId, roomId: client.roomId, members: roomInfo(client.roomId) }));
      broadcast(client.roomId, { type: "user_joined", id: clientId, username: client.username, members: roomInfo(client.roomId) }, clientId);
    }

    if (msg.type === "message") {
      if (!client.roomId) return;
      const text = (msg.text || "").trim().slice(0, 2000);
      if (!text) return;
      const payload = { type: "message", id: crypto.randomBytes(4).toString("hex"), senderId: clientId, username: client.username, text, time: new Date().toISOString() };
      ws.send(JSON.stringify({ ...payload, own: true }));
      broadcast(client.roomId, { ...payload, own: false }, clientId);
    }

    if (msg.type === "typing") {
      if (!client.roomId) return;
      broadcast(client.roomId, { type: "typing", senderId: clientId, username: client.username, isTyping: !!msg.isTyping }, clientId);
    }
  });

  ws.on("close", () => leaveRoom(client));
});

function leaveRoom(client) {
  if (!client.roomId) return;
  const members = rooms.get(client.roomId);
  if (members) {
    members.delete(client);
    if (members.size === 0) rooms.delete(client.roomId);
    else broadcast(client.roomId, { type: "user_left", id: client.id, username: client.username, members: roomInfo(client.roomId) });
  }
  client.roomId = null;
}

server.listen(PORT, () => console.log("Server running on port " + PORT));
