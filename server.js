// Serveur Resadresaka : signalisation PeerJS + hub (personnes connectées) + notifications push
const { PeerServer } = require('peer');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const os = require('os');
const path = require('path');

const port = Number(process.env.PORT) || 9000;

/* ---------- Notifications push (Web Push / VAPID) ---------- */
let webpush = null;
try { webpush = require('web-push'); } catch (e) { console.log('web-push absent : notifications push désactivées'); }
const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || 'BP67ODp2J1AoiETLof8efmIUqUOQ8sCBxyCZF_bj_U7qs5PLCPqSFz-8k_IbSHrisrWaBRsrSHZ1vyxb7O5fhVQ';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY || '';
const pushOn = !!(webpush && VAPID_PRIVATE);
if (pushOn) {
  try { webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'https://resadresak.netlify.app', VAPID_PUBLIC, VAPID_PRIVATE); }
  catch (e) { console.log('Clés VAPID invalides : ' + e.message); }
} else {
  console.log('Push désactivé (VAPID_PRIVATE_KEY manquante ou web-push non installé)');
}
const SUBS_FILE = process.env.SUBS_FILE || path.join(os.tmpdir(), 'resadresaka-subs.json');
const subs = new Map(); // uid -> { sec, sub, t }
try {
  const o = JSON.parse(fs.readFileSync(SUBS_FILE, 'utf8'));
  Object.keys(o).forEach(k => subs.set(k, o[k]));
  console.log('Abonnements push chargés : ' + subs.size);
} catch (e) {}
let saveT = null;
function saveSubs() {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    try { fs.writeFileSync(SUBS_FILE, JSON.stringify(Object.fromEntries(subs))); } catch (e) {}
  }, 2000);
}
// Seuls les vrais services push sont acceptés (évite que le serveur appelle une adresse quelconque)
const PUSH_HOSTS = /^https:\/\/(fcm\.googleapis\.com|android\.googleapis\.com|updates\.push\.services\.mozilla\.com|web\.push\.apple\.com|[a-z0-9.-]+\.push\.apple\.com|[a-z0-9.-]+\.notify\.windows\.com)\//;
const okSub = s => !!(s && typeof s.endpoint === 'string' && s.endpoint.length <= 700 && PUSH_HOSTS.test(s.endpoint) &&
  s.keys && typeof s.keys.p256dh === 'string' && typeof s.keys.auth === 'string' &&
  s.keys.p256dh.length <= 200 && s.keys.auth.length <= 100);
const pokeLast = new Map(); // "de>vers" -> timestamp
const pokeLog = new Map();  // de -> [timestamps]
const MAX_SUBS = 5000;

/* ---------- Modération : administrateur, blocages, signalements ---------- */
const crypto = require('crypto');
// Compte admin : identifiant + empreinte scrypt du mot de passe (le mot de passe n'est jamais écrit dans le code)
const ADMIN_LOGIN = 'christian';
const ADMIN_SALT = '020951f059839afe9c62396defc658ed';
const ADMIN_HASH = '80d13e2b6c5cecd7e4c60f3a0a340771e85fc1ce22571d26ecf1f4bebdac991a';
const MOD_FILE = process.env.MOD_FILE || path.join(os.tmpdir(), 'resadresaka-mod.json');
const bans = new Map();   // pid -> { pid, n, t }
let reports = [];         // { id, from, to, n, r, m, ts }
let repSeq = Date.now();
try {
  const o = JSON.parse(fs.readFileSync(MOD_FILE, 'utf8'));
  (o.bans || []).forEach(b => { if (b && b.pid) bans.set(b.pid, b); });
  reports = Array.isArray(o.reports) ? o.reports.slice(-100) : [];
  console.log('Modération : ' + bans.size + ' blocage(s), ' + reports.length + ' signalement(s)');
} catch (e) {}
let modT = null;
function saveMod() {
  clearTimeout(modT);
  modT = setTimeout(() => {
    try { fs.writeFileSync(MOD_FILE, JSON.stringify({ bans: [...bans.values()], reports })); } catch (e) {}
  }, 1000);
}
const PID_RE = /^[A-Za-z0-9_-]{3,64}$/;
function credOk(l, k) {
  if (typeof l !== 'string' || typeof k !== 'string' || l.length > 60 || k.length > 200) return false;
  const loginOk = l.trim().toLowerCase() === ADMIN_LOGIN;
  let h;
  try { h = crypto.scryptSync(k, ADMIN_SALT, 32); } catch (e) { return false; }
  return crypto.timingSafeEqual(h, Buffer.from(ADMIN_HASH, 'hex')) && loginOk;
}
let badKeys = [];          // tentatives de mot de passe ratées (limite globale : 8 par 10 min)
const repLog = new Map();  // pid -> [timestamps] (3 signalements max par 10 min)
function adminState() {
  return JSON.stringify({
    t: 'adm', ok: true,
    users: [...users.values()].map(u => ({ pid: u.pid, nick: u.nick })),
    reports: reports.slice(-50).reverse(),
    bans: [...bans.values()].map(b => ({ pid: b.pid, n: b.n }))
  });
}
function pushAdmins() {
  const m = adminState();
  hubWss.clients.forEach(c => { if (c.admin && c.readyState === 1) { try { c.send(m); } catch (e) {} } });
}
function handleReport(ws, d) {
  const from = ws.pid, to = String(d.to || '');
  const res = ok => reply(ws, { t: 'repres', ok });
  if (!from || !PID_RE.test(to) || to === from) return res(false);
  const n = Date.now(), log = (repLog.get(from) || []).filter(x => n - x < 600000);
  if (log.length >= 3) return res(false);
  log.push(n); repLog.set(from, log);
  const u = users.get(to);
  reports.push({
    id: ++repSeq, from, to,
    n: clean(d.n) || (u && u.nick) || 'Inconnu',
    r: String(d.r || '').slice(0, 200),
    m: (Array.isArray(d.m) ? d.m : []).slice(-10).map(x => String(x).slice(0, 200)),
    ts: n
  });
  if (reports.length > 100) reports.splice(0, reports.length - 100);
  saveMod(); pushAdmins(); res(true);
}
function handleAdmin(ws, d) {
  if (d.t === 'adm') {
    const n = Date.now();
    badKeys = badKeys.filter(x => n - x < 600000);
    if (badKeys.length >= 8 || !credOk(d.l, d.k)) {
      if (badKeys.length < 8) badKeys.push(n);
      return reply(ws, { t: 'adm', ok: false });
    }
    ws.admin = true;
    try { ws.send(adminState()); } catch (e) {}
    return;
  }
  if (!ws.admin) return;
  if (d.t === 'ban') {
    const pid = String(d.pid || '');
    if (!PID_RE.test(pid) || pid === ws.pid) return;
    const u = users.get(pid), rp = reports.find(r => r.to === pid);
    bans.set(pid, { pid, n: (u && u.nick) || (rp && rp.n) || 'Inconnu', t: Date.now() });
    if (subs.has(pid)) { subs.delete(pid); saveSubs(); }
    if (u) {
      const w = u.ws;
      reply(w, { t: 'banned' });
      users.delete(pid); w.pid = null;
      setTimeout(() => { try { w.close(4001, 'banned'); } catch (e) {} }, 300);
      broadcast();
    }
  } else if (d.t === 'unban') {
    bans.delete(String(d.pid || ''));
  } else if (d.t === 'dismiss') {
    reports = reports.filter(r => String(r.id) !== String(d.id));
  } else return;
  saveMod(); pushAdmins();
}

function reply(ws, o) { try { ws.send(JSON.stringify(o)); } catch (e) {} }

function handlePush(ws, d) {
  const pid = String(d.pid || ''), sec = String(d.sec || '');
  if (!pushOn) return reply(ws, { t: 'pushres', ok: false, r: 'off' });
  if (!/^rsk-[a-f0-9]{16}$/.test(pid) || !/^[A-Za-z0-9_-]{16,64}$/.test(sec) || ws.pid !== pid || !okSub(d.sub)) {
    return reply(ws, { t: 'pushres', ok: false, r: 'bad' });
  }
  const old = subs.get(pid);
  if (old && old.sec !== sec) return reply(ws, { t: 'pushres', ok: false, r: 'sec' });
  if (!old && subs.size >= MAX_SUBS) return reply(ws, { t: 'pushres', ok: false, r: 'full' });
  subs.set(pid, { sec, sub: { endpoint: d.sub.endpoint, keys: { p256dh: d.sub.keys.p256dh, auth: d.sub.keys.auth } }, t: Date.now() });
  saveSubs();
  reply(ws, { t: 'pushres', ok: true });
}

function handleUnpush(ws, d) {
  const pid = String(d.pid || ''), old = subs.get(pid);
  if (old && ws.pid === pid && old.sec === String(d.sec || '')) { subs.delete(pid); saveSubs(); }
}

function handlePoke(ws, d) {
  const to = String(d.to || ''), from = ws.pid;
  const res = r => reply(ws, { t: 'pokeres', to, r });
  if (!from || !/^rsk-[a-f0-9]{16}$/.test(to) || to === from) return;
  if (!pushOn) return res('off');
  const n = Date.now();
  const k = from + '>' + to;
  const log = (pokeLog.get(from) || []).filter(x => n - x < 600000);
  if (n - (pokeLast.get(k) || 0) < 60000 || log.length >= 10) return res('rate');
  if (users.has(to)) return res('online');
  const rec = subs.get(to);
  if (!rec) return res('nosub');
  pokeLast.set(k, n); log.push(n); pokeLog.set(from, log);
  const me = users.get(from);
  const payload = JSON.stringify({
    title: 'Resadresaka',
    body: ((me && me.nick) || 'Quelqu\'un') + ' veut discuter avec vous',
    tag: 'poke-' + from,
    url: './index.html?go=1'
  });
  webpush.sendNotification(rec.sub, payload, { TTL: 3600, urgency: 'high' })
    .then(() => res('ok'))
    .catch(err => {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) { subs.delete(to); saveSubs(); res('nosub'); }
      else { console.log('Échec push : ' + (err && (err.statusCode || err.message))); res('fail'); }
    });
}

/* ---------- Hub : registre des personnes connectées ---------- */
const hubWss = new WebSocketServer({ noServer: true, maxPayload: 8192 });
const users = new Map(); // pid -> { pid, nick, busy, seen, ws }
const MAX_CLIENTS = 1000;

const clean = x => String(x || '').replace(/[^\p{L}\p{N} _.-]/gu, '').trim().slice(0, 20);
const listMsg = () => JSON.stringify({
  t: 'list',
  u: [...users.values()].map(u => ({ pid: u.pid, nick: u.nick, busy: u.busy }))
});
function broadcast() {
  const msg = listMsg();
  hubWss.clients.forEach(c => { if (c.readyState === 1) { try { c.send(msg); } catch (e) {} } });
}

hubWss.on('connection', ws => {
  ws.isAlive = true;
  ws.admin = false;
  ws.pid = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});
  ws.on('message', raw => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!d) return;
    if (d.t === 'push') return handlePush(ws, d);
    if (d.t === 'unpush') return handleUnpush(ws, d);
    if (d.t === 'poke') return handlePoke(ws, d);
    if (d.t === 'report') return handleReport(ws, d);
    if (d.t === 'adm' || d.t === 'ban' || d.t === 'unban' || d.t === 'dismiss') return handleAdmin(ws, d);
    if (d.t !== 'reg') return;
    const pid = String(d.pid || '');
    if (!/^[A-Za-z0-9_-]{3,64}$/.test(pid)) return;
    if (bans.has(pid)) { // visiteur bloqué par l'administrateur
      reply(ws, { t: 'banned' });
      setTimeout(() => { try { ws.close(4001, 'banned'); } catch (e) {} }, 300);
      return;
    }
    const nick = clean(d.n) || 'Inconnu', busy = !!d.b;
    const prev = users.get(pid);
    if (prev && prev.ws !== ws) { // même identifiant qui se reconnecte : on ferme l'ancienne connexion
      prev.ws.pid = null;
      try { prev.ws.close(4000, 'replaced'); } catch (e) {}
    }
    if (ws.pid && ws.pid !== pid) users.delete(ws.pid);
    users.set(pid, { pid, nick, busy, seen: Date.now(), ws });
    ws.pid = pid;
    const changed = !prev || prev.ws !== ws || prev.nick !== nick || prev.busy !== busy;
    if (changed) broadcast(); else { try { ws.send(listMsg()); } catch (e) {} }
  });
  ws.on('close', () => {
    if (ws.pid && users.get(ws.pid) && users.get(ws.pid).ws === ws) {
      users.delete(ws.pid);
      broadcast();
    }
  });
});

// Toutes les 10 s : ping, nettoyage des connexions mortes, envoi de la liste (garde aussi la connexion active)
setInterval(() => {
  const n = Date.now();
  let changed = false;
  hubWss.clients.forEach(ws => {
    if (!ws.isAlive) { try { ws.terminate(); } catch (e) {} return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
  users.forEach((u, pid) => {
    if (n - u.seen > 30000) { users.delete(pid); try { u.ws.terminate(); } catch (e) {} changed = true; }
  });
  pokeLast.forEach((t, k) => { if (n - t > 120000) pokeLast.delete(k); });
  pokeLog.forEach((a, k) => { if (!a.some(x => n - x < 600000)) pokeLog.delete(k); });
  broadcast();
  if (changed) console.log('Hub : ' + users.size + ' connecté(s)');
}, 10000);

/* ---------- Serveur PeerJS (inchangé) + branchement du hub ---------- */
PeerServer({
  port,
  host: '0.0.0.0',          // exigé par Render pour détecter le port
  path: '/',                 // doit correspondre à "path" côté client
  proxied: true,             // indispensable derrière le proxy de Render
  allow_discovery: false,    // ne pas exposer la liste des identifiants
  alive_timeout: 60000,
  corsOptions: { origin: true }
}, server => {
  console.log('PeerServer démarré sur le port ' + port);
  if (!server || typeof server.listeners !== 'function') {
    console.log('Hub désactivé : serveur HTTP introuvable.');
    return;
  }

  // Requêtes HTTP : /health (réveil du serveur), le reste va à PeerJS
  const reqListeners = server.listeners('request');
  server.removeAllListeners('request');
  server.on('request', (req, res) => {
    const p = (req.url || '').split('?')[0];
    if (p === '/health' || p === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' });
      res.end('ok');
      return;
    }
    reqListeners.forEach(l => l.call(server, req, res));
  });

  // WebSocket : /hub va au hub, le reste va à PeerJS
  const upgListeners = server.listeners('upgrade');
  server.removeAllListeners('upgrade');
  server.on('upgrade', (req, socket, head) => {
    const p = (req.url || '').split('?')[0];
    if (p === '/hub') {
      if (hubWss.clients.size >= MAX_CLIENTS) { socket.destroy(); return; }
      hubWss.handleUpgrade(req, socket, head, ws => hubWss.emit('connection', ws, req));
      return;
    }
    upgListeners.forEach(l => l.call(server, req, socket, head));
  });
  console.log('Hub démarré sur /hub');
});
