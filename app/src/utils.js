const crypto = require('crypto');

function uuid() { return crypto.randomUUID(); }

function sha256Hex(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

module.exports = { uuid, sha256Hex };
