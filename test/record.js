const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const { record } = require('../index.js')

test('record codec round-trips sender, timestamp, and text', (t) => {
  const message = {
    sender: b4a.from('ab'.repeat(32), 'hex'),
    timestamp: 1763467200000,
    text: 'hello'
  }

  const encoded = c.encode(record, message)
  const decoded = c.decode(record, encoded)

  t.alike(decoded.sender, message.sender)
  t.is(decoded.timestamp, message.timestamp)
  t.is(decoded.text, message.text)
  t.is(Object.keys(decoded).length, 3)
})

test('record codec keeps field order stable', (t) => {
  const sender = b4a.alloc(32, 7)
  const encoded = c.encode(record, {
    sender,
    timestamp: 1,
    text: 'a'
  })

  t.alike(encoded.subarray(0, 32), sender)
  t.ok(encoded.byteLength > 32)
})
