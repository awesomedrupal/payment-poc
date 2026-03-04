const { withTx, query } = require('./db');
const { getKafka } = require('./kafka');

const topic = process.env.PAYMENTS_TOPIC || 'payments.events';
const pollMs = parseInt(process.env.OUTBOX_POLL_MS || '250', 10);
const batchSize = parseInt(process.env.OUTBOX_BATCH_SIZE || '200', 10);

async function main() {
  const kafka = getKafka();
  const producer = kafka.producer();
  await producer.connect();
  console.log('outbox_publisher connected');

  while (true) {
    try {
      const rows = await withTx(async (client) => {
        const { rows } = await client.query(
          `SELECT id, aggregate_id, event_type, payload
           FROM outbox
           WHERE status='PENDING'
           ORDER BY created_at
           LIMIT $1
           FOR UPDATE SKIP LOCKED`,
          [batchSize]
        );
        if (!rows.length) return [];
        const ids = rows.map(r => r.id);
        await client.query(`UPDATE outbox SET attempts=attempts+1 WHERE id = ANY($1::uuid[])`, [ids]);
        return rows;
      });

      for (const row of rows) {
        const evt = {
          outbox_id: row.id,
          payment_id: row.aggregate_id,
          event_type: row.event_type,
          payload: row.payload,
          published_at: new Date().toISOString()
        };

        await producer.send({
          topic,
          messages: [{ key: String(row.aggregate_id), value: JSON.stringify(evt) }]
        });

        await query(`UPDATE outbox SET status='SENT', sent_at=now() WHERE id=$1`, [row.id]);
      }

      if (!rows.length) await new Promise(r => setTimeout(r, pollMs));
    } catch (e) {
      console.error('publisher loop error', e);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
