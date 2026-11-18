const b4a = require('b4a')
const crypto = require('hypercore-crypto')

function toTopic (input) {
  if (b4a.isBuffer(input)) {
    if (input.byteLength !== 32) throw new Error('Topic must be 32 bytes')
    return input
  }

  if (typeof input !== 'string' || input.length === 0) {
    throw new Error('Topic must be a string or a 32-byte buffer')
  }

  if (/^[0-9a-f]{64}$/i.test(input)) return b4a.from(input, 'hex')
  return crypto.hash(b4a.from(input))
}

module.exports = toTopic
