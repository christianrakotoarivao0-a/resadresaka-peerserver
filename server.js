// Serveur de signalisation PeerJS pour Resadresaka
const { PeerServer } = require('peer');

const port = Number(process.env.PORT) || 9000;

PeerServer({
  port,
  host: '0.0.0.0',          // exigé par Render pour détecter le port
  path: '/',                 // doit correspondre à "path" côté client
  proxied: true,             // indispensable derrière le proxy de Render
  allow_discovery: false,    // ne pas exposer la liste des identifiants
  alive_timeout: 60000,
  corsOptions: { origin: true }
}, () => {
  console.log('PeerServer démarré sur le port ' + port);
});
