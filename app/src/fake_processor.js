const express = require('express');
const crypto = require('crypto');
const http = require('http');
const https = require('https');

const app = express();
app.use(express.json());

const port = parseInt(process.env.PORT || '4000', 10);
const webhookSecret = process.env.PROCESSOR_WEBHOOK_SECRET || 'processor_webhook_secret_dev';
const webhookTargetDefault = process.env.WEBHOOK_TARGET_URL || '';
const defaultOutcome = process.env.DEFAULT_OUTCOME || 'succeed';

const store = new Map(); // processor_payment_id -> record

function hmacBase64(secret, data) {
  return crypto.createHmac('sha256', secret).update(data).digest('base64');
}

function postWebhook(url, payload) {
  return new Promise((resolve, reject) => {
    const raw = JSON.stringify(payload);
    const sig = `v1=${hmacBase64(webhookSecret, raw)}`;

    const u = new URL(url);
    const data = Buffer.from(raw);
    const lib = u.protocol === 'https:' ? https : http;

    const req = lib.request({
      method: 'POST',
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
        'X-Processor-Signature': sig
      }
    }, (res) => {
      let out = '';
      res.on('data', c => out += c);
      res.on('end', () => resolve({ status: res.statusCode, body: out }));
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

app.get('/health', (_req, res) => res.json({ ok: true }));

app.post('/confirm', (req, res) => {
  const { payment_id, payment_method_token, webhook_url } = req.body || {};
  if (!payment_id || !payment_method_token) return res.status(400).json({ error: 'missing_fields' });

  const shouldFail = String(payment_method_token).endsWith('_fail');
  const final = shouldFail ? 'FAILED' : (defaultOutcome === 'fail' ? 'FAILED' : 'SUCCEEDED');

  const processor_payment_id = `pp_${crypto.randomUUID().replace(/-/g,'')}`;
  store.set(processor_payment_id, { payment_id, status: final, processor_payment_id });

  const target = webhook_url || webhookTargetDefault;
  const gateway_event_id = `evt_${crypto.randomUUID().replace(/-/g,'')}`;

  const payload = {
    gateway_event_id,
    processor_payment_id,
    payment_id,
    status: final,
    failure_code: final === 'FAILED' ? 'card_declined' : null,
    failure_message: final === 'FAILED' ? 'Card was declined (simulated)' : null
  };

  setTimeout(async () => {
    try {
      const r1 = await postWebhook(target, payload);
      const r2 = await postWebhook(target, payload); // duplicate on purpose
      console.log('fake_processor webhook', final, 'sent', r1.status, 'dup', r2.status);
    } catch (e) {
      console.error('fake_processor webhook failed', e);
    }
  }, 800);

  return res.json({ processor_payment_id, status: 'PROCESSING' });
});

app.get('/payments/:processor_payment_id', (req, res) => {
  const rec = store.get(req.params.processor_payment_id);
  if (!rec) return res.status(404).json({ error: 'not_found' });
  return res.json(rec);
});

app.get('/settlement_report', (_req, res) => {
  const items = [];
  for (const rec of store.values()) {
    if (rec.status === 'SUCCEEDED') items.push({ processor_payment_id: rec.processor_payment_id, payment_id: rec.payment_id, status: 'SUCCEEDED' });
  }
  return res.json({ generated_at: new Date().toISOString(), items });
});

app.listen(port, () => console.log(`fake_processor listening on ${port}`));
