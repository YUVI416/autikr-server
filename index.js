const express = require('express');
const cors = require('cors');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, makeCacheableSignalKeyStore } = require('@whiskeysockets/baileys');
const pino = require('pino');
const Groq = require('groq-sdk');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 7878;
const logger = pino({ level: 'silent' });

// ─── State ──────────────────────────────────────────────────────
let sock = null;
let isConnected = false;
let isConnecting = false;
let autoReply = false;
let sseClients = [];
let lastPairingCode = '';
let settings = {
  groqKey: '', geminiKey: '',
  systemPrompt: 'You are a highly intelligent and polite personal AI assistant. Reply to incoming WhatsApp messages naturally and concisely on my behalf. Keep responses friendly and brief unless detailed information is requested. Match the language of the incoming message.',
  replyToGroups: false, replyDelay: 2
};
let stats = { received: 0, replied: 0, errors: 0 };

// ─── AI Reply ───────────────────────────────────────────────────
async function generateAIReply(message) {
  const prompt = settings.systemPrompt;
  if (settings.groqKey) {
    try {
      const groq = new Groq({ apiKey: settings.groqKey });
      const r = await groq.chat.completions.create({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'system', content: prompt }, { role: 'user', content: message }],
        temperature: 0.7, max_tokens: 500
      });
      const reply = r.choices?.[0]?.message?.content;
      if (reply) return reply.trim();
    } catch (e) { console.log('[AI] Groq error:', e.message); }
  }
  if (settings.geminiKey) {
    try {
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${settings.geminiKey}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system_instruction: { parts: [{ text: prompt }] }, contents: [{ parts: [{ text: message }] }], generationConfig: { temperature: 0.7, maxOutputTokens: 500 } })
      });
      const data = await res.json();
      const reply = data.candidates?.[0]?.content?.parts?.[0]?.text;
      if (reply) return reply.trim();
    } catch (e) { console.log('[AI] Gemini error:', e.message); }
  }
  throw new Error('No API keys configured');
}

// ─── SSE Broadcast ──────────────────────────────────────────────
function broadcast(type, data = {}) {
  const payload = JSON.stringify({ type, ...data, time: Date.now() });
  sseClients.forEach(res => { try { res.write(`data: ${payload}\n\n`); } catch(e) {} });
}

// ─── WhatsApp Connection ────────────────────────────────────────
async function connectWhatsApp(phoneNumber) {
  if (isConnecting) {
    console.log('[WA] Already connecting, skipping...');
    return;
  }
  
  if (sock) { try { sock.end(); } catch(e) {} sock = null; }
  
  isConnecting = true;
  const cleanNum = phoneNumber.replace(/[^0-9]/g, '');
  console.log('[WA] Starting connection for:', cleanNum);

  // Ensure auth_session directory exists
  const fs = require('fs');
  const path = require('path');
  const authDir = path.join(process.cwd(), 'auth_session');
  if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(authDir);

  sock = makeWASocket({
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    logger, printQRInTerminal: false,
    browser: ['Autikr', 'Chrome', '120.0.0'],
    connectTimeoutMs: 60000
  });

  // Request pairing code after WebSocket is ready
  if (!state.creds.registered) {
    setTimeout(async () => {
      if (!sock) return;
      try {
        console.log('[WA] Requesting pairing code for:', cleanNum);
        const code = await sock.requestPairingCode(cleanNum);
        lastPairingCode = code;
        console.log('[WA] ✅ Pairing code:', code);
        broadcast('pairing_code', { code });
      } catch (e) {
        console.error('[WA] Pairing code error:', e.message);
        broadcast('error', { message: 'Pairing failed: ' + e.message });
        isConnecting = false;
      }
    }, 8000); // Wait 8 seconds for WS to stabilize
  } else {
    console.log('[WA] Already registered, connecting...');
  }

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect } = update;
    console.log('[WA] Connection update:', connection);
    
    if (connection === 'close') {
      isConnected = false;
      isConnecting = false;
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      console.log('[WA] Closed, status:', statusCode);
      broadcast('wa_status', { status: 'disconnected' });
      
      if (statusCode === 405 || statusCode === DisconnectReason.loggedOut) {
        // 405 = pairing rejected, clear and let user try again
        console.log('[WA] Pairing rejected or logged out, clearing session');
        try { fs.rmSync('./auth_session', { recursive: true, force: true }); } catch(e) {}
        broadcast('error', { message: 'Pairing failed (405). Tap "Get Pairing Code" again.' });
      } else if (statusCode === DisconnectReason.restartRequired) {
        console.log('[WA] Restart required, reconnecting in 5s...');
        setTimeout(() => connectWhatsApp(phoneNumber), 5000);
      } else if (state.creds.registered && statusCode !== DisconnectReason.loggedOut) {
        console.log('[WA] Reconnecting in 10s...');
        setTimeout(() => connectWhatsApp(phoneNumber), 10000);
      }
    } else if (connection === 'open') {
      isConnected = true;
      isConnecting = false;
      console.log('[WA] ✅ Connected successfully!');
      broadcast('wa_status', { status: 'connected' });
    }
  });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (msg.key.fromMe) continue;
      const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || '';
      if (!text) continue;

      const isGroup = msg.key.remoteJid?.endsWith('@g.us');
      const from = msg.key.remoteJid;

      stats.received++;
      broadcast('message', { from, body: text, isGroup });

      if (!autoReply) continue;
      if (isGroup && !settings.replyToGroups) continue;

      try {
        const reply = await generateAIReply(text);
        await new Promise(r => setTimeout(r, (settings.replyDelay || 2) * 1000));
        await sock.sendMessage(from, { text: reply });
        stats.replied++;
        broadcast('reply', { to: from, body: reply });
        console.log('[WA] Replied to', from.split('@')[0]);
      } catch (e) {
        stats.errors++;
        broadcast('error', { message: e.message });
      }
    }
  });
}

// ─── API Routes ─────────────────────────────────────────────────
app.get('/api/ping', (_, res) => res.json({ ok: true, server: 'Autikr' }));

app.post('/api/connect', async (req, res) => {
  const { phone } = req.body;
  if (!phone) return res.json({ success: false, message: 'Phone number required' });
  if (isConnected) return res.json({ success: true, message: 'Already connected!' });
  
  // Force clear any stuck state
  if (sock) { try { sock.end(); } catch(e) {} sock = null; }
  isConnecting = false;
  lastPairingCode = '';
  
  // Clear old session files (not folder) for fresh pairing
  const fs = require('fs');
  const path = require('path');
  const authDir = path.join(process.cwd(), 'auth_session');
  if (fs.existsSync(authDir)) {
    const files = fs.readdirSync(authDir);
    files.forEach(f => { try { fs.unlinkSync(path.join(authDir, f)); } catch(e) {} });
  }
  
  try {
    await connectWhatsApp(phone);
    res.json({ success: true, message: 'Connecting... wait ~8 seconds for pairing code' });
  } catch (e) {
    isConnecting = false;
    res.json({ success: false, message: e.message });
  }
});

app.post('/api/reset', async (_, res) => {
  if (sock) { try { sock.end(); } catch(e) {} sock = null; }
  isConnected = false; isConnecting = false; lastPairingCode = '';
  const fs = require('fs');
  try { fs.rmSync('./auth_session', { recursive: true, force: true }); } catch(e) {}
  res.json({ success: true, message: 'Session cleared' });
});

app.post('/api/disconnect', async (_, res) => {
  if (sock) { try { await sock.logout(); } catch(e) {} sock = null; }
  isConnected = false; isConnecting = false;
  const fs = require('fs');
  try { fs.rmSync('./auth_session', { recursive: true, force: true }); } catch(e) {}
  res.json({ success: true });
});

app.get('/api/status', (_, res) => {
  res.json({ whatsapp: isConnected ? 'connected' : 'disconnected', autoReply, stats, groq: settings.groqKey ? 'ready' : 'unconfigured', gemini: settings.geminiKey ? 'ready' : 'unconfigured' });
});

app.post('/api/autoreply', (req, res) => {
  autoReply = req.body.active;
  broadcast('autoreply', { active: autoReply });
  res.json({ success: true, active: autoReply });
});

app.get('/api/settings', (_, res) => res.json(settings));
app.post('/api/settings', (req, res) => {
  Object.assign(settings, req.body);
  res.json({ success: true });
});

app.post('/api/ai/chat', async (req, res) => {
  try {
    const reply = await generateAIReply(req.body.message);
    res.json({ success: true, reply });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/api/events', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Access-Control-Allow-Origin': '*' });
  res.write(`data: ${JSON.stringify({ type: 'connected' })}\n\n`);
  // Send current status
  if (isConnected) res.write(`data: ${JSON.stringify({ type: 'wa_status', status: 'connected' })}\n\n`);
  if (lastPairingCode && !isConnected) res.write(`data: ${JSON.stringify({ type: 'pairing_code', code: lastPairingCode })}\n\n`);
  sseClients.push(res);
  req.on('close', () => { sseClients = sseClients.filter(c => c !== res); });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n⚡ AUTIKR SERVER running on port ${PORT}`);
  console.log(`📱 Mobile app can connect to this server\n`);
});
