const express = require('express');
const { hmacAuth } = require('./auth_middleware');
const { withTx, query } = require('./db');
const { uuid, sha256Hex } = require('./utils');
const { hmacBase64 } = require('./hmac');

const app = express();

// Capture raw body for hashing/signature verification
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = buf.toString('utf8'); }
}));

app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/time', (_req, res) => res.json({ server_time_unix: Math.floor(Date.now()/1000) }));

// Merchant: create PaymentIntent
app.post('/payment_intents', hmacAuth, async (req, res) => {
  const merchantId = req.merchant.id;
  const { amount_cents, currency, order_ref } = req.body || {};
  const idem = req.header('Idempotency-Key') || '';

  if (!idem) return res.status(400).json({ error: 'missing_idempotency_key' });
  if (!amount_cents || !currency) return res.status(400).json({ error: 'missing_amount_or_currency' });

  const clientSecret = `cs_${uuid().replace(/-/g,'')}`;
  const clientSecretHash = sha256Hex(clientSecret);

  try {
    const result = await withTx(async (client) => {
      const existing = await client.query(
        `SELECT id, status FROM payment_intents WHERE merchant_id=$1 AND idempotency_key=$2`,
        [merchantId, idem]
      );
      if (existing.rows.length) {
        return { payment_intent_id: existing.rows[0].id, status: existing.rows[0].status, client_secret: null, idempotent: true };
      }

      const paymentId = uuid();
      await client.query(
        `INSERT INTO payment_intents
         (id, merchant_id, amount_cents, currency, status, order_ref, idempotency_key, client_secret_hash, client_secret_expires_at)
         VALUES ($1,$2,$3,$4,'REQUIRES_PAYMENT_METHOD',$5,$6,$7, now() + interval '60 minutes')`,
        [paymentId, merchantId, amount_cents, currency, order_ref || null, idem, clientSecretHash]
      );

      await client.query(
        `INSERT INTO payment_events (id, payment_id, type, from_status, to_status, source, payload)
         VALUES ($1,$2,'PAYMENT_INTENT_CREATED',NULL,'REQUIRES_PAYMENT_METHOD','api',$3::jsonb)`,
        [uuid(), paymentId, JSON.stringify({ amount_cents, currency, order_ref })]
      );

      await client.query(
        `INSERT INTO outbox (id, aggregate_id, event_type, payload, dedupe_key)
         VALUES ($1,$2,'PAYMENT_INTENT_CREATED',$3::jsonb,$4)`,
        [uuid(), paymentId, JSON.stringify({ payment_id: paymentId, merchant_id: merchantId }), `${paymentId}:PAYMENT_INTENT_CREATED`]
      );

      return { payment_intent_id: paymentId, status: 'REQUIRES_PAYMENT_METHOD', client_secret: clientSecret, idempotent: false };
    });

    return res.status(201).json(result);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'create_failed' });
  }
});

// Merchant: get status
app.get('/payment_intents/:id', hmacAuth, async (req, res) => {
  const merchantId = req.merchant.id;
  const id = req.params.id;

  const { rows } = await query(
    `SELECT id, merchant_id, amount_cents, currency, status, order_ref, processor_payment_id, failure_code, failure_message, created_at, updated_at
     FROM payment_intents WHERE id=$1 AND merchant_id=$2`,
    [id, merchantId]
  );
  if (!rows.length) return res.status(404).json({ error: 'not_found' });
  return res.json(rows[0]);
});

// Customer: confirm (client_secret + token)
app.post('/payment_intents/:id/confirm', async (req, res) => {
  const id = req.params.id;
  const { client_secret, payment_method_token } = req.body || {};
  if (!client_secret || !payment_method_token) return res.status(400).json({ error: 'missing_client_secret_or_token' });

  try {
    const out = await withTx(async (client) => {
      const { rows } = await client.query(
        `SELECT id, status, client_secret_hash, client_secret_expires_at FROM payment_intents WHERE id=$1`,
        [id]
      );
      if (!rows.length) return { status: 404, body: { error: 'not_found' } };

      const pi = rows[0];
      if (new Date(pi.client_secret_expires_at).getTime() < Date.now()) return { status: 401, body: { error: 'client_secret_expired' } };
      if (sha256Hex(client_secret) !== pi.client_secret_hash) return { status: 401, body: { error: 'invalid_client_secret' } };

      if (['SUCCEEDED','FAILED','CANCELED'].includes(pi.status)) {
        return { status: 200, body: { payment_intent_id: id, status: pi.status, note: 'terminal' } };
      }

      await client.query(`UPDATE payment_intents SET status='PROCESSING', updated_at=now() WHERE id=$1`, [id]);

      await client.query(
        `INSERT INTO payment_events (id, payment_id, type, from_status, to_status, source, payload)
         VALUES ($1,$2,'CONFIRM_REQUESTED',$3,'PROCESSING','api',$4::jsonb)`,
        [uuid(), id, pi.status, JSON.stringify({ payment_method_token })]
      );

      await client.query(
        `INSERT INTO outbox (id, aggregate_id, event_type, payload, dedupe_key)
         VALUES ($1,$2,'PAYMENT_CONFIRM_REQUESTED',$3::jsonb,$4)`,
        [uuid(), id, JSON.stringify({ payment_id: id, payment_method_token }), `${id}:PAYMENT_CONFIRM_REQUESTED:${Date.now()}`]
      );

      return { status: 200, body: { payment_intent_id: id, status: 'PROCESSING' } };
    });

    return res.status(out.status).json(out.body);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'confirm_failed' });
  }
});

// Processor webhook (authoritative)
app.post('/webhooks/gateway', async (req, res) => {
  const secret = process.env.PROCESSOR_WEBHOOK_SECRET || '';
  const sig = req.header('X-Processor-Signature') || '';
  const raw = req.rawBody || '';
  const expected = `v1=${hmacBase64(secret, raw)}`;
  if (sig !== expected) return res.status(401).json({ error: 'bad_webhook_signature' });

  const { gateway_event_id, processor_payment_id, payment_id, status, failure_code, failure_message } = req.body || {};
  if (!gateway_event_id || !payment_id || !status) return res.status(400).json({ error: 'missing_fields' });

  try {
    await withTx(async (client) => {
      try {
        await client.query(
          `INSERT INTO webhook_events (gateway_event_id, processor_payment_id, raw_payload)
           VALUES ($1,$2,$3::jsonb)`,
          [gateway_event_id, processor_payment_id || null, JSON.stringify(req.body)]
        );
      } catch (e) {
        if (String(e.code) === '23505') return; // duplicate
        throw e;
      }

      const { rows } = await client.query(`SELECT status FROM payment_intents WHERE id=$1 FOR UPDATE`, [payment_id]);
      if (!rows.length) return;
      const current = rows[0].status;
      if (['SUCCEEDED','FAILED','CANCELED'].includes(current)) return;

      let nextStatus = null;
      if (status === 'SUCCEEDED') nextStatus = 'SUCCEEDED';
      if (status === 'FAILED') nextStatus = 'FAILED';
      if (!nextStatus) return;

      await client.query(
        `UPDATE payment_intents
         SET status=$2, processor_payment_id=COALESCE($3, processor_payment_id),
             failure_code=COALESCE($4, failure_code),
             failure_message=COALESCE($5, failure_message),
             updated_at=now()
         WHERE id=$1`,
        [payment_id, nextStatus, processor_payment_id || null, failure_code || null, failure_message || null]
      );

      await client.query(
        `INSERT INTO payment_events (id, payment_id, type, from_status, to_status, source, payload)
         VALUES ($1,$2,$3,$4,$5,'webhook',$6::jsonb)`,
        [uuid(), payment_id, nextStatus === 'SUCCEEDED' ? 'GATEWAY_SUCCEEDED' : 'GATEWAY_FAILED',
         current, nextStatus,
         JSON.stringify({ gateway_event_id, processor_payment_id, failure_code, failure_message })]
      );

      await client.query(
        `INSERT INTO outbox (id, aggregate_id, event_type, payload, dedupe_key)
         VALUES ($1,$2,$3,$4::jsonb,$5)`,
        [uuid(), payment_id, nextStatus === 'SUCCEEDED' ? 'PAYMENT_SUCCEEDED' : 'PAYMENT_FAILED',
         JSON.stringify({ payment_id, status: nextStatus }),
         `${payment_id}:${nextStatus}:${gateway_event_id}`]
      );
    });

    return res.json({ ok: true });
  } catch (e) {
    console.error('webhook error', e);
    return res.status(500).json({ error: 'webhook_processing_failed' });
  }
});

const port = parseInt(process.env.PORT || '3000', 10);
app.listen(port, () => console.log(`api listening on ${port}`));

setInterval(() => { query(`DELETE FROM used_nonces WHERE expires_at < now()`).catch(() => {}); }, 30_000);
