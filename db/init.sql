-- Merchants + Keys
CREATE TABLE merchants (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE merchant_api_keys (
  api_key TEXT NOT NULL,
  key_id TEXT NOT NULL,
  merchant_id UUID NOT NULL REFERENCES merchants(id),
  api_secret TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  PRIMARY KEY (api_key, key_id)
);

-- Replay protection (mock Redis)
CREATE TABLE used_nonces (
  merchant_id UUID NOT NULL REFERENCES merchants(id),
  nonce TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (merchant_id, nonce)
);
CREATE INDEX used_nonces_expires_idx ON used_nonces(expires_at);

-- Core payment tables
CREATE TABLE payment_intents (
  id UUID PRIMARY KEY,
  merchant_id UUID NOT NULL REFERENCES merchants(id),
  amount_cents BIGINT NOT NULL,
  currency TEXT NOT NULL,
  status TEXT NOT NULL,
  order_ref TEXT,
  idempotency_key TEXT NOT NULL,
  client_secret_hash TEXT NOT NULL,
  client_secret_expires_at TIMESTAMPTZ NOT NULL,
  processor_payment_id TEXT,
  failure_code TEXT,
  failure_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (merchant_id, idempotency_key)
);

CREATE TABLE payment_events (
  id UUID PRIMARY KEY,
  payment_id UUID NOT NULL REFERENCES payment_intents(id),
  type TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  source TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  network TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX payment_events_payment_time_idx ON payment_events(payment_id, created_at DESC);

-- Webhook dedupe
CREATE TABLE webhook_events (
  gateway_event_id TEXT PRIMARY KEY,
  processor_payment_id TEXT,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Outbox
CREATE TABLE outbox (
  id UUID PRIMARY KEY,
  aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'PENDING',
  dedupe_key TEXT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at TIMESTAMPTZ,
  UNIQUE (dedupe_key)
);
CREATE INDEX outbox_pending_idx ON outbox(status, created_at);

-- Reconciliation
CREATE TABLE reconciliation_cases (
  id UUID PRIMARY KEY,
  payment_id UUID NOT NULL REFERENCES payment_intents(id),
  processor_payment_id TEXT NOT NULL,
  internal_status TEXT NOT NULL,
  processor_status TEXT NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  state TEXT NOT NULL DEFAULT 'OPEN',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ
);
CREATE INDEX reconciliation_cases_state_idx ON reconciliation_cases(state, created_at);

-- Seed merchant + key
INSERT INTO merchants (id, name) VALUES
('11111111-1111-1111-1111-111111111111', 'Demo Merchant');

INSERT INTO merchant_api_keys (api_key, key_id, merchant_id, api_secret, status) VALUES
('mk_test_demo', 'key_v1', '11111111-1111-1111-1111-111111111111', 'sk_test_supersecret', 'ACTIVE');
