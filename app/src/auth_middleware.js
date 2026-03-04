const { query } = require('./db');
const { sha256Base64, hmacBase64, timingSafeEqual, canonicalString } = require('./hmac');

function pathWithQuery(req) {
  return req.originalUrl.split('#')[0];
}

async function hmacAuth(req, res, next) {
  try {
    const apiKey = req.header('X-Api-Key');
    const keyId = req.header('X-Key-Id');
    const timestamp = req.header('X-Timestamp');
    const nonce = req.header('X-Nonce');
    const contentSha256 = req.header('X-Content-SHA256');
    const sigHeader = req.header('X-Signature') || '';

    if (!apiKey || !keyId || !timestamp || !nonce || !contentSha256 || !sigHeader) {
      return res.status(401).json({ error: 'missing_auth_headers' });
    }

    const now = Math.floor(Date.now() / 1000);
    const skew = parseInt(process.env.HMAC_SKEW_SECONDS || '300', 10);
    const ts = parseInt(timestamp, 10);
    if (!Number.isFinite(ts) || Math.abs(now - ts) > skew) {
      res.set('X-Server-Time', String(now));
      return res.status(401).json({ error: 'clock_skew', allowed_skew_seconds: skew });
    }

    const { rows } = await query(
      `SELECT merchant_id, api_secret, status FROM merchant_api_keys WHERE api_key=$1 AND key_id=$2`,
      [apiKey, keyId]
    );
    if (!rows.length) return res.status(401).json({ error: 'invalid_key' });
    const { merchant_id: merchantId, api_secret: secret, status } = rows[0];
    if (status !== 'ACTIVE') return res.status(401).json({ error: 'key_disabled' });

    const rawBody = req.rawBody || '';
    const computedBodyHash = sha256Base64(rawBody);
    if (!timingSafeEqual(computedBodyHash, contentSha256)) {
      return res.status(401).json({ error: 'body_hash_mismatch' });
    }

    const canon = canonicalString({
      method: req.method,
      pathWithQuery: pathWithQuery(req),
      timestamp,
      nonce,
      contentSha256
    });
    const expected = `v1=${hmacBase64(secret, canon)}`;
    if (!timingSafeEqual(expected, sigHeader)) {
      return res.status(401).json({ error: 'bad_signature' });
    }

    // Nonce replay protection (POC via Postgres). Swap to Redis SET NX EX in real systems.
    const ttl = parseInt(process.env.NONCE_TTL_SECONDS || '600', 10);
    try {
      await query(
        `INSERT INTO used_nonces(merchant_id, nonce, expires_at)
         VALUES ($1,$2, now() + ($3 || ' seconds')::interval)`,
        [merchantId, nonce, ttl]
      );
    } catch (e) {
      if (String(e.code) === '23505') return res.status(401).json({ error: 'replay_detected' });
      throw e;
    }

    query(`UPDATE merchant_api_keys SET last_used_at=now() WHERE api_key=$1 AND key_id=$2`, [apiKey, keyId]).catch(() => {});
    req.merchant = { id: merchantId, apiKey, keyId };
    return next();
  } catch (e) {
    console.error('hmacAuth error', e);
    return res.status(500).json({ error: 'auth_internal_error' });
  }
}

module.exports = { hmacAuth };
