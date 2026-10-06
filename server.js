// Drive & Fly Online - tiny relay server.
// Serves index.html and relays player state between everyone in the same room.
// Run:  npm install  &&  npm start     (then open http://localhost:3000)

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const MAX_PER_ROOM = 16;

const server = http.createServer((req, res) => {
  const url = req.url.split("?")[0];
  if (url === "/" || url === "/index.html") {
    fs.readFile(path.join(__dirname, "index.html"), (err, data) => {
      if (err) { res.writeHead(500); return res.end("index.html not found next to server.js"); }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(data);
    });
  } else if (url === "/health") {
    res.writeHead(200); res.end("ok");
  } else {
    res.writeHead(404); res.end("Not found");
  }
});

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 2048 });
const rooms = new Map(); // roomName -> Set<ws>

const num = (v, lim = 6000) => (Number.isFinite(v) ? Math.max(-lim, Math.min(lim, v)) : 0);

// Only forward known, sanitized fields.
function clean(m, id) {
  return {
    t: "s", id,
    n: String(m.n || "Player").replace(/[<>&"']/g, "").slice(0, 14),
    c: /^#[0-9a-f]{6}$/i.test(m.c) ? m.c : "#ffffff",
    m: [0, 1, 2].includes(m.m) ? m.m : 0,
    x: num(m.x), y: num(m.y, 1500), z: num(m.z),
    yw: num(m.yw, 20), p: num(m.p, 4), r: num(m.r, 4), s: num(m.s, 300),
    b1: num(m.b1, 3600000), b2: num(m.b2, 3600000)
  };
}

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

wss.on("connection", (ws) => {
  ws.id = Math.random().toString(36).slice(2, 8);
  ws.room = null;
  ws.alive = true;
  ws.msgCount = 0;

  ws.on("pong", () => { ws.alive = true; });

  ws.on("message", (raw) => {
    if (++ws.msgCount > 40) return;           // ~40 msgs/sec cap (reset below)
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "join") {
      if (ws.room) return;
      const name = String(m.room || "main").toLowerCase().replace(/[^a-z0-9_-]/g, "").slice(0, 12) || "main";
      let set = rooms.get(name);
      if (!set) { set = new Set(); rooms.set(name, set); }
      if (set.size >= MAX_PER_ROOM) { send(ws, { t: "full" }); return ws.close(); }
      ws.room = name; set.add(ws);
      send(ws, { t: "welcome", id: ws.id, room: name, players: set.size });
    } else if (m.t === "s" && ws.room) {
      const out = JSON.stringify(clean(m, ws.id));
      for (const peer of rooms.get(ws.room)) {
        if (peer !== ws && peer.readyState === 1) peer.send(out);
      }
    } else if (m.t === "ping") {
      send(ws, { t: "pong", c: m.c });
    }
  });

  ws.on("close", () => {
    if (!ws.room) return;
    const set = rooms.get(ws.room);
    if (!set) return;
    set.delete(ws);
    for (const peer of set) send(peer, { t: "leave", id: ws.id });
    if (set.size === 0) rooms.delete(ws.room);
  });
});

setInterval(() => { wss.clients.forEach((c) => { c.msgCount = 0; }); }, 1000);
setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.alive) return c.terminate();
    c.alive = false; c.ping();
  });
}, 15000);

server.listen(PORT, () => console.log("Drive & Fly server running on port " + PORT));
