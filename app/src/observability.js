const client = require("prom-client");
const http = require("http");
const crypto = require("crypto");

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const httpRequestsTotal = new client.Counter({
  name: "payment_poc_http_requests_total",
  help: "HTTP requests total",
  labelNames: ["service", "method", "route", "status_code"],
  registers: [register],
});

const httpRequestDurationMs = new client.Histogram({
  name: "payment_poc_http_request_duration_ms",
  help: "HTTP request duration in ms",
  labelNames: ["service", "method", "route", "status_code"],
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000],
  registers: [register],
});

const paymentTransitionsTotal = new client.Counter({
  name: "payment_poc_payment_transitions_total",
  help: "Payment state transitions",
  labelNames: ["service", "from_status", "to_status", "source"],
  registers: [register],
});

const webhookDuplicatesTotal = new client.Counter({
  name: "payment_poc_webhook_duplicates_total",
  help: "Duplicate webhooks ignored",
  labelNames: ["service"],
  registers: [register],
});

const webhookEventsTotal = new client.Counter({
  name: "payment_poc_webhook_events_total",
  help: "Webhook events processed",
  labelNames: ["service", "status"],
  registers: [register],
});

const outboxRowsPublishedTotal = new client.Counter({
  name: "payment_poc_outbox_rows_published_total",
  help: "Outbox rows published",
  labelNames: ["service", "event_type"],
  registers: [register],
});

const processorCallsTotal = new client.Counter({
  name: "payment_poc_processor_calls_total",
  help: "Processor calls",
  labelNames: ["service", "result"],
  registers: [register],
});

const reconciliationCorrectionsTotal = new client.Counter({
  name: "payment_poc_reconciliation_corrections_total",
  help: "Reconciliation corrections applied",
  labelNames: ["service"],
  registers: [register],
});

function newTraceId() {
  return crypto.randomUUID().replace(/-/g, "");
}

function childSpanId() {
  return crypto.randomUUID().replace(/-/g, "").slice(0, 16);
}

function createLogger(service) {
  return function log(level, message, fields = {}) {
    console.log(
      JSON.stringify({
        ts: new Date().toISOString(),
        level,
        service,
        message,
        ...fields,
      }),
    );
  };
}

function tracingMiddleware(service, logger) {
  return function (req, res, next) {
    const traceId = req.header("x-trace-id") || newTraceId();
    const spanId = childSpanId();
    req.trace = { trace_id: traceId, span_id: spanId, service };

    const start = Date.now();
    res.setHeader("x-trace-id", traceId);

    res.on("finish", () => {
      const route = req.route && req.route.path ? req.route.path : req.path;
      const ms = Date.now() - start;

      httpRequestsTotal.inc({
        service,
        method: req.method,
        route,
        status_code: String(res.statusCode),
      });

      httpRequestDurationMs.observe(
        {
          service,
          method: req.method,
          route,
          status_code: String(res.statusCode),
        },
        ms,
      );

      logger("info", "http_request", {
        trace_id: traceId,
        span_id: spanId,
        method: req.method,
        route,
        status_code: res.statusCode,
        duration_ms: ms,
      });
    });

    next();
  };
}

function startMetricsServer(port, logger) {
  const server = http.createServer(async (req, res) => {
    if (req.url === "/metrics") {
      res.statusCode = 200;
      res.setHeader("Content-Type", register.contentType);
      res.end(await register.metrics());
      return;
    }
    if (req.url === "/healthz") {
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    res.statusCode = 404;
    res.end("not found");
  });

  server.listen(port, () => logger("info", "metrics_server_started", { port }));
}

module.exports = {
  register,
  createLogger,
  tracingMiddleware,
  startMetricsServer,
  metrics: {
    httpRequestsTotal,
    httpRequestDurationMs,
    paymentTransitionsTotal,
    webhookDuplicatesTotal,
    webhookEventsTotal,
    outboxRowsPublishedTotal,
    processorCallsTotal,
    reconciliationCorrectionsTotal,
  },
  newTraceId,
  childSpanId,
};
