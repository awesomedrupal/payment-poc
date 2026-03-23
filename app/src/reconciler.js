const http = require("http");
const https = require("https");
const { withTx, query } = require("./db");
const {
  createLogger,
  startMetricsServer,
  metrics,
  newTraceId,
} = require("./observability");
const crypto = require("crypto");

const service = "reconciler";
const log = createLogger(service);
startMetricsServer(parseInt(process.env.METRICS_PORT || "9103", 10), log);

const processorUrl = process.env.FAKE_PROCESSOR_URL || "http://localhost:4000";
const everyMs = parseInt(process.env.RECONCILE_EVERY_MS || "5000", 10);

function getJson(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === "https:" ? https : http;
    const req = lib.request(
      {
        method: "GET",
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: { Accept: "application/json" },
      },
      (res) => {
        let out = "";
        res.on("data", (c) => (out += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(out || "{}"));
          } catch (e) {
            reject(e);
          }
        });
      },
    );
    req.on("error", reject);
    req.end();
  });
}

async function reconcileOnce() {
  const traceId = newTraceId();
  const report = await getJson(`${processorUrl}/settlement_report`);
  const items = report.items || [];
  if (!items.length) return;
  const succeeded = new Map(
    items.map((i) => [i.payment_id, i.processor_payment_id]),
  );
  const { rows } = await query(
    `SELECT id, status FROM payment_intents WHERE status IN ('FAILED','PROCESSING')`,
  );
  for (const pi of rows) {
    const processorPid = succeeded.get(pi.id);
    if (!processorPid) continue;
    if (pi.status === "FAILED") {
      const caseId = crypto.randomUUID();
      await withTx(async (client) => {
        await client.query(
          `INSERT INTO reconciliation_cases (id, payment_id, processor_payment_id, internal_status, processor_status, evidence) VALUES ($1,$2,$3,$4,'SUCCEEDED',$5::jsonb)`,
          [
            caseId,
            pi.id,
            processorPid,
            pi.status,
            JSON.stringify({
              report_generated_at: report.generated_at,
              trace_id: traceId,
            }),
          ],
        );
        await client.query(
          `UPDATE payment_intents SET status='SUCCEEDED', processor_payment_id=$2, updated_at=now() WHERE id=$1 AND status='FAILED'`,
          [pi.id, processorPid],
        );
        await client.query(
          `INSERT INTO payment_events (id, payment_id, type, from_status, to_status, source, payload) VALUES ($1,$2,'RECONCILIATION_CORRECTION_APPLIED','FAILED','SUCCEEDED','reconciliation',$3::jsonb)`,
          [
            crypto.randomUUID(),
            pi.id,
            JSON.stringify({
              case_id: caseId,
              processor_payment_id: processorPid,
              trace_id: traceId,
            }),
          ],
        );
        await client.query(
          `INSERT INTO outbox (id, aggregate_id, event_type, payload, dedupe_key) VALUES ($1,$2,'PAYMENT_STATUS_CORRECTED',$3::jsonb,$4)`,
          [
            crypto.randomUUID(),
            pi.id,
            JSON.stringify({
              payment_id: pi.id,
              from: "FAILED",
              to: "SUCCEEDED",
              case_id: caseId,
              trace_id: traceId,
            }),
            `${pi.id}:PAYMENT_STATUS_CORRECTED:${caseId}`,
          ],
        );
        await client.query(
          `UPDATE reconciliation_cases SET state='RESOLVED', resolved_at=now() WHERE id=$1`,
          [caseId],
        );
      });
      metrics.reconciliationCorrectionsTotal.inc({ service });
      metrics.paymentTransitionsTotal.inc({
        service,
        from_status: "FAILED",
        to_status: "SUCCEEDED",
        source: "reconciliation",
      });
      log("info", "reconciliation_corrected", {
        trace_id: traceId,
        payment_id: pi.id,
        processor_payment_id: processorPid,
        case_id: caseId,
      });
    }
  }
}

async function main() {
  log("info", "reconciler_started", { every_ms: everyMs });
  while (true) {
    try {
      await reconcileOnce();
    } catch (e) {
      log("error", "reconcile_error", { error: e.message });
    }
    await new Promise((r) => setTimeout(r, everyMs));
  }
}

main().catch((e) => {
  log("error", "reconciler_fatal", { error: e.message });
  process.exit(1);
});
