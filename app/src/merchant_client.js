const crypto = require('crypto');
const http = require('http');

const API_BASE = process.env.API_BASE || 'http://localhost:3000';
const API_KEY = process.env.MERCHANT_API_KEY || 'mk_test_demo';
const KEY_ID = process.env.MERCHANT_KEY_ID || 'key_v1';
const SECRET = process.env.MERCHANT_API_SECRET || 'sk_test_supersecret';

function sha256Base64(data) { return crypto.createHash('sha256').update(data).digest('base64'); }
function hmacBase64(secret, data) { return crypto.createHmac('sha256', secret).update(data).digest('base64'); }
function uuid() { return crypto.randomUUID(); }
function canonical(method, path, ts, nonce, bodyHash) { return `${method.toUpperCase()}
${path}
${ts}
${nonce}
${bodyHash}`; }

function merchantRequest(method, path, bodyObj, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const body = bodyObj ? JSON.stringify(bodyObj) : '';
    const ts = Math.floor(Date.now()/1000).toString();
    const nonce = uuid();
    const bodyHash = sha256Base64(body);
    const sig = `v1=${hmacBase64(SECRET, canonical(method, path, ts, nonce, bodyHash))}`;

    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      'X-Api-Key': API_KEY,
      'X-Key-Id': KEY_ID,
      'X-Timestamp': ts,
      'X-Nonce': nonce,
      'X-Content-SHA256': bodyHash,
      'X-Signature': sig,
      ...extraHeaders
    };

    const u = new URL(API_BASE);
    const req = http.request({ method, hostname: u.hostname, port: u.port, path, headers }, (res) => {
      let out = '';
      res.on('data', c => out += c);
      res.on('end', () => resolve({ status: res.statusCode, body: out, headers: res.headers }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function customerConfirm(id, clientSecret, token) {
  return new Promise((resolve, reject) => {
    const u = new URL(API_BASE);
    const body = JSON.stringify({ client_secret: clientSecret, payment_method_token: token });
    const req = http.request({
      method: 'POST',
      hostname: u.hostname,
      port: u.port,
      path: `/payment_intents/${id}/confirm`,
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      let out = '';
      res.on('data', c => out += c);
      res.on('end', () => resolve({ status: res.statusCode, body: out }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function main() {
  const cmd = process.argv[2] || 'create';

  if (cmd === 'create') {
    const idem = uuid();
    const resp = await merchantRequest('POST', '/payment_intents', { amount_cents: 1299, currency: 'USD', order_ref: 'order-123' }, { 'Idempotency-Key': idem });
    console.log('CREATE', resp.status, resp.body);
    console.log('Idempotency-Key used:', idem);
    return;
  }

  if (cmd === 'get') {
    const id = process.argv[3];
    if (!id) throw new Error('usage: node merchant_client.js get <payment_intent_id>');
    const resp = await merchantRequest('GET', `/payment_intents/${id}`, null);
    console.log('GET', resp.status, resp.body);
    return;
  }

  if (cmd === 'confirm') {
    const id = process.argv[3];
    const cs = process.argv[4];
    const token = process.argv[5] || 'tok_test_ok';
    if (!id || !cs) throw new Error('usage: node merchant_client.js confirm <payment_intent_id> <client_secret> [token]');
    const resp = await customerConfirm(id, cs, token);
    console.log('CONFIRM', resp.status, resp.body);
    return;
  }

  console.log('Use: create | get | confirm');
}

main().catch(e => { console.error(e); process.exit(1); });
