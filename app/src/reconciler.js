const http = require('http');
const https = require('https');
const { withTx, query } = require('./db');
const { uuid } = require('uuid');

const processorUrl = process.env.FAKE_PROCESSOR_URL || 'http://localhost:4000';
const everyMs = parseInt(process.env.RECONCILE_EVERY_MS || '5000', 10);

function getJson(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const req = lib.request({
      method: 'GET',
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      headers: { 'Accept': 'application/json' }
    }, (res) => {
      let out = '';
      res.on('data', c => out += c);
      res.on('end', () => {
        try { resolve(JSON.parse(out || '{}')); } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function reconcileOnce() {
  const report = await getJson(`${processorUrl}/settlement_report`);
  const items = report.items || [];
  if (!items.length) return;

  const succeeded = new Map(items.map(i => [i.payment_id, i.processor_payment_id]));
  const { rows } = await query(`SELECT id, status FROM payment_intents WHERE status IN ('FAILED','PROCESSING')`);

  for (const pi of rows) {
    const processorPid = succeeded.get(pi.id);
    if (!processorPid) continue;

    if (pi.status === 'FAILED') {
      const caseId = require('crypto').randomUUID();
      await withTx(async (client) => {
        await client.query(
          `INSERT INTO reconciliation_cases (id, payment_id, processor_payment_id, internal_status, processor_status, evidence)
           VALUES ($1,$2,$3,$4,'SUCCEEDED',$5::jsonb)`,
          [caseId, pi.id, processorPid, pi.status, JSON.stringify({ report_generated_at: report.generated_at })]
        );

        await client.query(
          `UPDATE payment_intents SET status='SUCCEEDED', processor_payment_id=$2, updated_at=now()
           WHERE id=$1 AND status='FAILED'`,
          [pi.id, processorPid]
        );

        await client.query(
          `INSERT INTO payment_events (id, payment_id, type, from_status, to_status, source, payload)
           VALUES ($1,$2,'RECONCILIATION_CORRECTION_APPLIED','FAILED','SUCCEEDED','reconciliation',$3::jsonb)`,
          [require('crypto').randomUUID(), pi.id, JSON.stringify({ case_id: caseId, processor_payment_id: processorPid })]
        );

        await client.query(
          `INSERT INTO outbox (id, aggregate_id, event_type, payload, dedupe_key)
           VALUES ($1,$2,'PAYMENT_STATUS_CORRECTED',$3::jsonb,$4)`,
          [require('crypto').randomUUID(), pi.id, JSON.stringify({ payment_id: pi.id, from: 'FAILED', to: 'SUCCEEDED', case_id: caseId }),
           `${pi.id}:PAYMENT_STATUS_CORRECTED:${caseId}`]
        );

        await client.query(`UPDATE reconciliation_cases SET state='RESOLVED', resolved_at=now() WHERE id=$1`, [caseId]);
      });

      console.log('reconciler corrected FAILED->SUCCEEDED for payment', pi.id);
    }
  }
}

async function main() {
  console.log('reconciler running every', everyMs, 'ms');
  while (true) {
    try { await reconcileOnce(); } catch (e) { console.error('reconcile error', e); }
    await new Promise(r => setTimeout(r, everyMs));
  }
}

main().catch(e => { console.error(e); process.exit(1); });
