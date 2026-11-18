const c = require('compact-encoding')

// On-disk chat record. Field order is the schema: do not reorder or insert.
// sender is the writer's 32-byte Hypercore public key.
// timestamp is milliseconds since the Unix epoch, as a compact uint.
// text is a UTF-8 string.
const record = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.sender)
    c.uint.preencode(state, m.timestamp)
    c.string.preencode(state, m.text)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.sender)
    c.uint.encode(state, m.timestamp)
    c.string.encode(state, m.text)
  },
  decode (state) {
    return {
      sender: c.fixed32.decode(state),
      timestamp: c.uint.decode(state),
      text: c.string.decode(state)
    }
  }
}

module.exports = record
