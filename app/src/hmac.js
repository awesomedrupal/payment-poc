const crypto = require('crypto');

function sha256Base64(data) {
  return crypto.createHash('sha256').update(data).digest('base64');
}

function hmacBase64(secret, data) {
  return crypto.createHmac('sha256', secret).update(data).digest('base64');
}

function timingSafeEqual(a, b) {
  const aa = Buffer.from(a);
  const bb = Buffer.from(b);
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function canonicalString({ method, pathWithQuery, timestamp, nonce, contentSha256 }) {
  return `${method.toUpperCase()}
${pathWithQuery}
${timestamp}
${nonce}
${contentSha256}`;
}

module.exports = { sha256Base64, hmacBase64, timingSafeEqual, canonicalString };
