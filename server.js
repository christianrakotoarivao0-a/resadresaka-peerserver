// Serveur Resadresaka : signalisation PeerJS + hub (liste des personnes connectées)
const { PeerServer } = require('peer');
const { WebSocketServer } = require('ws');

const port = Number(process.env.PORT) || 9000;

/* ---------- Hub : registre des personnes connectées ---------- */
const hubWss = new WebSocketServer({ noServer: true, maxPayload: 2048 });
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
  ws.pid = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});
  ws.on('message', raw => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!d || d.t !== 'reg') return;
    const pid = String(d.pid || '');
    if (!/^[A-Za-z0-9_-]{3,64}$/.test(pid)) return;
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
