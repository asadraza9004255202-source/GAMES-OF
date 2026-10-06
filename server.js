// Drive & Fly Online - tiny relay server.
// Serves index.html and relays player state between everyone in the same room.
// Run:  npm install  &&  npm start     (then open http://localhost:3000)

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const MAX_PER_ROOM = 5;

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
const normalizeRoom = (raw) => String(raw || "main").toLowerCase().replace(/[^a-z0-9_-]/g, "").slice(0, 12) || "main";
const sanitizeName = (v) => String(v || "Player").replace(/[<>&"']/g, "").slice(0, 14) || "Player";
const sanitizeColor = (v) => /^#[0-9a-f]{6}$/i.test(v) ? v : "#ffffff";

function clean(m, id) {
  return {
    t: "s", id,
    n: sanitizeName(m.n),
    c: sanitizeColor(m.c),
    m: [0, 1, 2].includes(m.m) ? m.m : 0,
    x: num(m.x), y: num(m.y, 1500), z: num(m.z),
    yw: num(m.yw, 20), p: num(m.p, 4), r: num(m.r, 4), s: num(m.s, 300),
    b1: num(m.b1, 3600000), b2: num(m.b2, 3600000)
  };
}

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function removeFromRoom(ws) {
  if (!ws.room) return;
  const set = rooms.get(ws.room);
  if (!set) { ws.room = null; return; }
  set.delete(ws);
  for (const peer of set) send(peer, { t: "leave", id: ws.id });
  if (set.size === 0) rooms.delete(ws.room);
  ws.room = null;
}
function addToRoom(ws, room) {
  const safeRoom = normalizeRoom(room);
  if (ws.room && ws.room !== safeRoom) removeFromRoom(ws);
  if (ws.room === safeRoom) return rooms.get(safeRoom);
  let set = rooms.get(safeRoom);
  if (!set) { set = new Set(); rooms.set(safeRoom, set); }
  if (set.size >= MAX_PER_ROOM) { send(ws, { t: "full" }); return null; }
  ws.room = safeRoom;
  set.add(ws);
  return set;
}
function findClientByUid(uid) {
  const targetUid = Number(uid);
  if (!Number.isInteger(targetUid)) return null;
  for (const client of wss.clients) {
    if (client.uid === targetUid) return client;
  }
  return null;
}

wss.on("connection", (ws) => {
  ws.id = Math.random().toString(36).slice(2, 8);
  ws.uid = 1000 + Math.floor(Math.random() * 9000);
  ws.room = null;
  ws.profile = { name: "Player", color: "#ffffff" };
  ws.alive = true;
  ws.msgCount = 0;

  send(ws, { t: "profile_ok", uid: ws.uid });

  ws.on("pong", () => { ws.alive = true; });

  ws.on("message", (raw) => {
    if (++ws.msgCount > 40) return;
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "profile") {
      ws.profile = {
        name: sanitizeName(m.name),
        color: sanitizeColor(m.color)
      };
      send(ws, { t: "profile_ok", uid: ws.uid });
    } else if (m.t === "join") {
      const room = normalizeRoom(m.room);
      const set = addToRoom(ws, room);
      if (!set) return;
      send(ws, { t: "welcome", id: ws.id, uid: ws.uid, room, players: set.size });
    } else if (m.t === "s" && ws.room) {
      const out = JSON.stringify(clean(m, ws.id));
      for (const peer of rooms.get(ws.room)) {
        if (peer !== ws && peer.readyState === 1) peer.send(out);
      }
    } else if (m.t === "invite") {
      const toUid = Number(m.toUid);
      const room = normalizeRoom(m.room);
      if (!Number.isInteger(toUid) || toUid === ws.uid) {
        send(ws, { t: "invite_error", msg: "Invalid UID." });
        return;
      }
      const target = findClientByUid(toUid);
      if (!target) {
        send(ws, { t: "invite_error", msg: "User not online." });
        return;
      }
      target.send(JSON.stringify({
        t: "invite",
        fromUid: ws.uid,
        fromName: ws.profile?.name || "Player",
        room
      }));
      send(ws, { t: "invite_sent", toUid, room });
    } else if (m.t === "accept_invite") {
      const fromUid = Number(m.fromUid);
      const room = normalizeRoom(m.room);
      if (!Number.isInteger(fromUid)) {
        send(ws, { t: "invite_error", msg: "Invalid sender UID." });
        return;
      }
      const sender = findClientByUid(fromUid);
      if (!sender) {
        send(ws, { t: "invite_error", msg: "Invite ka sender online nahi hai." });
        return;
      }
      const set = addToRoom(ws, room);
      if (!set) {
        send(ws, { t: "invite_error", msg: "Room full hai." });
        return;
      }
      send(ws, { t: "welcome", id: ws.id, uid: ws.uid, room, players: set.size });
      sender.send(JSON.stringify({ t: "invite_accepted", name: ws.profile?.name || "Player", room }));
    } else if (m.t === "ping") {
      send(ws, { t: "pong", c: m.c });
    }
  });

  ws.on("close", () => {
    removeFromRoom(ws);
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
