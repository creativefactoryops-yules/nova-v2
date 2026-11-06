/**
 * NOVA v2 — WebSocket Backend with Streaming
 * --force ./buildyou · terminal forest edition
 *
 * HTTP REST  →  site data, docs, history (CRUD)
 * WebSocket  →  streaming chat, live site-update push
 */

require("dotenv").config();
const http       = require("http");
const express    = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const Anthropic  = require("@anthropic-ai/sdk");
const cors       = require("cors");
const helmet     = require("helmet");
const morgan     = require("morgan");
const rateLimit  = require("express-rate-limit");
const { v4: uuidv4 } = require("uuid");
const fs         = require("fs");
const path       = require("path");

const PORT        = process.env.PORT        || 3001;
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || null;
const DATA_DIR    = path.resolve(process.env.DATA_DIR || "./data");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const SITE_FILE    = path.join(DATA_DIR, "site.json");
const DOCS_FILE    = path.join(DATA_DIR, "docs.json");
const HISTORY_FILE = path.join(DATA_DIR, "history.json");

const DEFAULT_SITE = {
  settings: {
    brandName: "--force ./buildyou", tagline: "rm -rf doubt",
    primaryColor: "#3DFF73", accentColor: "#b5ff4d",
    email: "forcebuildyou@gmail.com", phone: "", address: "",
  },
  hero: {
    heading: "Terminal Forest Websites",
    subheading: "AI-managed sites. Chat to update. Live in seconds.",
    ctaText: "Get Started", ctaSecondary: "See How",
  },
  about: {
    heading: "About",
    body: "NOVA is an AI website admin. Talk to it, and your site updates in real time.",
    highlight: "Local-first · Private · Streaming",
  },
  services: [
    { icon: "❯", title: "Chat Updates",    description: "Tell NOVA what to change. It updates your site live." },
    { icon: "◈", title: "RAG Knowledge",   description: "Feed it docs. It retrieves context automatically." },
    { icon: "◆", title: "Streaming",       description: "Replies stream word-by-word over WebSocket." },
  ],
  contact: {
    heading: "Let's Build",
    subtext: "Ready? Talk to NOVA.",
    buttonText: "Start Chatting",
  },
  _meta: { updatedAt: new Date().toISOString(), version: 1 },
};

const readJSON  = (file, fallback) => {
  try { if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch (e) { console.error(`[read] ${file}:`, e.message); }
  return typeof fallback === "function" ? fallback() : fallback;
};
const writeJSON = (file, data) => {
  try { fs.writeFileSync(file, JSON.stringify(data, null, 2), "utf8"); return true; }
  catch (e) { console.error(`[write] ${file}:`, e.message); return false; }
};

function applyPatch(obj, pathStr, value) {
  const clone = JSON.parse(JSON.stringify(obj));
  const keys  = pathStr.split(".");
  let cur = clone;
  for (let i = 0; i < keys.length - 1; i++) {
    const k = isNaN(keys[i]) ? keys[i] : +keys[i];
    if (cur[k] === undefined) cur[k] = isNaN(keys[i + 1]) ? {} : [];
    cur = cur[k];
  }
  const last = keys[keys.length - 1];
  cur[isNaN(last) ? last : +last] = value;
  return clone;
}
function patchSiteData(updates) {
  let site = readJSON(SITE_FILE, DEFAULT_SITE);
  for (const [p, v] of Object.entries(updates)) site = applyPatch(site, p, v);
  site._meta = { updatedAt: new Date().toISOString(), version: (site._meta?.version || 0) + 1 };
  writeJSON(SITE_FILE, site);
  return site;
}

function ragRetrieve(query, docs) {
  if (!docs?.length) return "";
  const words   = query.toLowerCase().split(/\s+/).filter(w => w.length > 3);
  const scored  = docs
    .map(d => ({ ...d, score: words.reduce((s, w) => s + (d.content.toLowerCase().includes(w) ? 1 : 0), 0) }))
    .filter(d => d.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);
  if (!scored.length) return "";
  return "\n\n## RETRIEVED KNOWLEDGE BASE\n" + scored.map(d => `### ${d.title}\n${d.content.slice(0, 800)}`).join("\n\n");
}

function buildSystemPrompt(ragCtx = "") {
  const site = readJSON(SITE_FILE, DEFAULT_SITE);
  return `You are NOVA — an AI website admin for --force ./buildyou. You manage a live website through conversation. Be direct, write great copy, lowercase terminal-forest voice.

## CURRENT WEBSITE DATA
\`\`\`json
${JSON.stringify(site, null, 2)}
\`\`\`

## HOW TO UPDATE
End your reply with a <site_update> block when the user wants a change:
<site_update>
{"path.to.update": "new value"}
</site_update>

## PATHS
settings.brandName | settings.tagline | settings.primaryColor | settings.accentColor | settings.email | settings.phone | settings.address
hero.heading | hero.subheading | hero.ctaText | hero.ctaSecondary
about.heading | about.body | about.highlight
services.0.title | services.0.description | services.0.icon (etc for .1, .2)
contact.heading | contact.subtext | contact.buttonText

## RULES
- Direct and confident, no filler
- Confirm every change after making it
- Write specific, energetic copy — never generic
- Suggest one follow-up improvement after each change${ragCtx}`;
}

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: CORS_ORIGIN, credentials: true }));
app.use(express.json({ limit: "2mb" }));
app.use(morgan("dev"));
app.use(express.static(path.join(__dirname, "public")));

const apiLimiter = rateLimit({ windowMs: 60_000, max: 120, message: { error: "Rate limit" } });
app.use("/api/", apiLimiter);

function requireAuth(req, res, next) {
  if (!ADMIN_TOKEN) return next();
  if (req.headers["x-admin-token"] !== ADMIN_TOKEN) return res.status(401).json({ error: "Unauthorized" });
  next();
}

app.get("/api/health", (req, res) =>
  res.json({ status: "ok", service: "NOVA v2", ws: true, streaming: true, timestamp: new Date().toISOString() })
);
app.get("/api/site", (req, res) => res.json({ success: true, data: readJSON(SITE_FILE, DEFAULT_SITE) }));
app.put("/api/site", requireAuth, (req, res) => {
  const data = { ...req.body, _meta: { updatedAt: new Date().toISOString(), version: (req.body._meta?.version || 0) + 1 } };
  writeJSON(SITE_FILE, data);
  broadcast({ type: "site_updated", payload: { site: data, updatedPaths: ["*"] } });
  res.json({ success: true, data });
});
app.patch("/api/site", requireAuth, (req, res) => {
  const updates = req.body;
  if (!updates || typeof updates !== "object") return res.status(400).json({ error: "Invalid body" });
  const site = patchSiteData(updates);
  broadcast({ type: "site_updated", payload: { site, updatedPaths: Object.keys(updates) } });
  res.json({ success: true, data: site, updatedPaths: Object.keys(updates) });
});
app.post("/api/site/reset", requireAuth, (req, res) => {
  writeJSON(SITE_FILE, DEFAULT_SITE);
  broadcast({ type: "site_updated", payload: { site: DEFAULT_SITE, updatedPaths: ["*"] } });
  res.json({ success: true, data: DEFAULT_SITE });
});
app.get("/api/docs", (req, res) => res.json({ success: true, data: readJSON(DOCS_FILE, []) }));
app.post("/api/docs", requireAuth, (req, res) => {
  const { title, content } = req.body;
  if (!title?.trim() || !content?.trim()) return res.status(400).json({ error: "title and content required" });
  const docs = readJSON(DOCS_FILE, []);
  const doc  = { id: uuidv4(), title: title.trim(), content: content.trim(), createdAt: new Date().toISOString(), chars: content.trim().length };
  docs.push(doc); writeJSON(DOCS_FILE, docs);
  res.status(201).json({ success: true, data: doc });
});
app.delete("/api/docs/:id", requireAuth, (req, res) => {
  const docs = readJSON(DOCS_FILE, []);
  const filtered = docs.filter(d => d.id !== req.params.id);
  if (filtered.length === docs.length) return res.status(404).json({ error: "Not found" });
  writeJSON(DOCS_FILE, filtered);
  res.json({ success: true, remaining: filtered.length });
});
app.get("/api/history", (req, res) => res.json({ success: true, data: readJSON(HISTORY_FILE, []) }));
app.delete("/api/history", requireAuth, (req, res) => { writeJSON(HISTORY_FILE, []); res.json({ success: true }); });

const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: "/ws" });
const clients = new Map();

function broadcast(msg, excludeId = null) {
  const data = JSON.stringify(msg);
  for (const [id, ws] of clients) {
    if (id !== excludeId && ws.readyState === WebSocket.OPEN) {
      try { ws.send(data); } catch {}
    }
  }
}
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) { try { ws.send(JSON.stringify(msg)); } catch {} }
}

wss.on("connection", (ws) => {
  const clientId = uuidv4().slice(0, 8);
  clients.set(clientId, ws);
  console.log(`[ws] +client ${clientId} (total: ${clients.size})`);
  send(ws, { type: "connected", payload: { clientId, version: "2.0" } });
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return send(ws, { type: "error", payload: { message: "Invalid JSON" } }); }
    if (msg.type === "ping") return send(ws, { type: "pong" });

    if (msg.type === "chat") {
      const { message, history = [] } = msg.payload || {};
      if (!message?.trim()) return send(ws, { type: "error", payload: { message: "Empty message" } });

      const docs   = readJSON(DOCS_FILE, []);
      const ragCtx = ragRetrieve(message, docs);
      const system = buildSystemPrompt(ragCtx);
      const apiMessages = [
        ...history.slice(-20).map(m => ({ role: m.role, content: m.content })),
        { role: "user", content: message },
      ];
      send(ws, { type: "stream_start" });
      let fullText = "";
      try {
        const stream = anthropic.messages.stream({ model: "claude-sonnet-4-6", max_tokens: 1000, system, messages: apiMessages });
        for await (const event of stream) {
          if (event.type === "content_block_delta" && event.delta?.type === "text_delta") {
            fullText += event.delta.text;
            send(ws, { type: "stream_delta", payload: { text: event.delta.text } });
          }
        }
        let siteUpdates = null, displayText = fullText;
        const match = fullText.match(/<site_update>([\s\S]*?)<\/site_update>/);
        if (match) {
          try {
            siteUpdates = JSON.parse(match[1].trim());
            const updatedSite = patchSiteData(siteUpdates);
            broadcast({ type: "site_updated", payload: { site: updatedSite, updatedPaths: Object.keys(siteUpdates) } });
          } catch (e) { console.warn("[ws] site_update parse fail:", e.message); }
          displayText = fullText.replace(/<site_update>[\s\S]*?<\/site_update>/, "").trim();
        }
        send(ws, { type: "stream_end", payload: { fullText: displayText, siteUpdates } });
        const chatHistory = readJSON(HISTORY_FILE, []);
        chatHistory.push(
          { role: "user", content: message, timestamp: new Date().toISOString() },
          { role: "assistant", content: displayText, timestamp: new Date().toISOString(), siteUpdates }
        );
        if (chatHistory.length > 200) chatHistory.splice(0, chatHistory.length - 200);
        writeJSON(HISTORY_FILE, chatHistory);
      } catch (err) {
        console.error("[ws] stream error:", err.message);
        send(ws, { type: "error", payload: { message: "Stream error: " + err.message } });
      }
      return;
    }
    send(ws, { type: "error", payload: { message: `Unknown type: ${msg.type}` } });
  });

  ws.on("close", () => { clients.delete(clientId); console.log(`[ws] -client ${clientId} (total: ${clients.size})`); });
  ws.on("error", (err) => { console.error(`[ws] error ${clientId}:`, err.message); clients.delete(clientId); });
});

const heartbeat = setInterval(() => {
  for (const [id, ws] of clients) {
    if (!ws.isAlive) { ws.terminate(); clients.delete(id); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30_000);
wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`\n  ❯ NOVA v2 running on http://localhost:${PORT}`);
  console.log(`  ⚡ WebSocket: ws://localhost:${PORT}/ws`);
  console.log(`  ◆ Data dir: ${DATA_DIR}`);
  console.log(`  ✓ Anthropic: ${process.env.ANTHROPIC_API_KEY ? "configured" : "MISSING — add to .env"}\n`);
});
