const { getKafka } = require('./kafka');
const http = require('http');
const https = require('https');

const topic = process.env.PAYMENTS_TOPIC || 'payments.events';
const processorUrl = process.env.FAKE_PROCESSOR_URL || 'http://localhost:4000';
const serviceBaseUrl = process.env.SERVICE_BASE_URL || 'http://localhost:3000';

function postJson(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const data = Buffer.from(JSON.stringify(body));
    const lib = u.protocol === 'https:' ? https : http;

    const req = lib.request({
      method: 'POST',
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      headers: { 'Content-Type': 'application/json', 'Content-Length': data.length, ...headers }
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

async function main() {
  const kafka = getKafka();
  const consumer = kafka.consumer({ groupId: 'orchestrator' });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log('orchestrator consuming', topic);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const evt = JSON.parse(message.value.toString('utf8'));
      if (evt.event_type !== 'PAYMENT_CONFIRM_REQUESTED') return;

      const payment_id = evt.payment_id;
      const token = evt.payload.payment_method_token;

      const resp = await postJson(`${processorUrl}/confirm`, {
        payment_id,
        payment_method_token: token,
        webhook_url: `${serviceBaseUrl}/webhooks/gateway`
      });
      console.log('orchestrator -> processor confirm', payment_id, resp.status);
    }
  });
}

main().catch(e => { console.error(e); process.exit(1); });
