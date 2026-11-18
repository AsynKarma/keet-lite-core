const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const tmp = require('test-tmp')
const KeetLite = require('../index.js')

test('two cores on one topic replicate a record', { timeout: 120000 }, async (t) => {
  const testnet = await createTestnet(3)
  const a = await createChat(t, testnet)
  const b = await createChat(t, testnet)
  t.teardown(() => testnet.destroy(), { order: Infinity })

  const heard = []
  const peers = []
  b.onMessage((message) => heard.push(message))
  b.onPeer((peer) => peers.push(peer))

  await a.join('lobby')
  await b.join('lobby')
  const peer = await waitFor(() => peers[0])
  t.is(peer.remotePublicKey.byteLength, 32)

  await a.send('hello from a')

  const message = await waitFor(() => heard.find((item) => item.text === 'hello from a'))
  t.alike(message.sender, a.publicKey)
  t.alike(message.key, a.publicKey)
  t.is(message.seq, 0)
  t.ok(message.timestamp > 0)

  const onB = await b.history({ key: a.publicKey, start: 0, end: 1 })
  t.is(onB.length, 1)
  t.is(onB[0].text, 'hello from a')

  await b.send('one')
  await b.send('two')
  await waitFor(() => heard.length >= 1)

  const page = await b.history({ start: 0, end: 2, limit: 1 })
  const own = page.filter((item) => b4a.equals(item.key, b.publicKey))
  t.is(own.length, 1)
  t.is(own[0].text, 'two')
  t.is(own[0].seq, 1)
})

test('stop then join re-announces the topic', { timeout: 120000 }, async (t) => {
  const testnet = await createTestnet(3)
  const a = await createChat(t, testnet)
  const b = await createChat(t, testnet)
  t.teardown(() => testnet.destroy(), { order: Infinity })

  const heard = []
  a.onMessage((message) => heard.push(message.text))

  await a.join('lobby')
  await b.join('lobby')
  await b.send('before')
  await waitFor(() => heard.includes('before'))

  await a.stop()
  t.is(a.swarm.suspended, true)

  await b.send('while-stopped')
  await delay(300)
  t.absent(heard.includes('while-stopped'))

  await a.join('lobby')
  t.is(a.swarm.suspended, false)

  await waitFor(() => heard.includes('while-stopped'))

  const history = await a.history({ key: b.publicKey })
  t.alike(history.map((item) => item.text), ['before', 'while-stopped'])
})

async function createChat (t, testnet) {
  const storage = await tmp(t)
  const chat = new KeetLite(storage, { bootstrap: testnet.bootstrap })
  t.teardown(() => chat.close())
  return chat
}

async function waitFor (fn, timeout = 20000) {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    const value = fn()
    if (value) return value
    await delay(50)
  }
  throw new Error('timed out')
}

function delay (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
