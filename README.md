# Payment System POC (Node + Postgres + Kafka + Reconciliation)

Runnable local POC implementing key patterns:
- Merchant HMAC auth (timestamp window + nonce replay protection)
- PaymentIntent + client_secret confirm flow (customer)
- Append-only payment_events audit log
- webhook_events dedupe
- Outbox pattern publishing to Kafka
- Orchestrator consumes Kafka and calls a fake processor
- Fake processor sends signed webhooks (including duplicates)
- Reconciler consumes settlement report and applies compensating corrections

## Run
```bash
docker compose up --build
```

API: http://localhost:3000  
Fake processor: http://localhost:4000

## Use cases to test

### 1) Create PaymentIntent (merchant HMAC + idempotency)
```bash
docker compose exec api node src/merchant_client.js create
```
Save the printed `payment_intent_id` and `client_secret` (client_secret is returned only once).

### 2) Confirm (customer) -> PROCESSING, then webhook finalizes
```bash
docker compose exec api node src/merchant_client.js confirm <payment_intent_id> <client_secret> tok_test_ok
```

Wait ~1s, then:
```bash
docker compose exec api node src/merchant_client.js get <payment_intent_id>
```

### 3) Failure path
```bash
docker compose exec api node src/merchant_client.js confirm <payment_intent_id> <client_secret> tok_test_fail
```

## Inspect DB
```bash
docker compose exec postgres psql -U pay -d paydb
```

```sql
SELECT * FROM payment_intents ORDER BY created_at DESC LIMIT 5;
SELECT type, from_status, to_status, source, created_at
FROM payment_events
WHERE payment_id='<id>'
ORDER BY created_at;
SELECT status, count(*) FROM outbox GROUP BY 1;
SELECT * FROM webhook_events ORDER BY received_at DESC LIMIT 5;
SELECT * FROM reconciliation_cases ORDER BY created_at DESC LIMIT 5;
```

## Notes
- Nonce replay protection uses Postgres `used_nonces` for simplicity. Replace with Redis `SET NX EX` in real systems.
- Kafka topic `payments.events` is auto-created by the broker in this POC.
- Webhook signature header: `X-Processor-Signature: v1=<base64(hmac(secret, rawBody))>`

---

## Push to GitHub

### Option A: From your laptop (recommended)
1) Unzip this repo
2) Initialize git and make first commit:
```bash
git init
git add .
git commit -m "Initial payment system POC"
```

3) Create a new empty repo in GitHub (no README/license; you already have them), then:
```bash
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

### Option B: GitHub UI upload
- Create a new repo in GitHub
- Use **Add file → Upload files**
- Upload everything from this folder

---

## Dev loop (optional)
If you want to run components outside Docker (faster iteration):
```bash
cp .env.example .env
# update DATABASE_URL/KAFKA_BROKERS if needed
cd app
npm install
npm run api
```
(Repeat for publisher/orchestrator/processor/reconciler in separate terminals.)
