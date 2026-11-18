#!/usr/bin/env node

const process = require('process')
const b4a = require('b4a')
const KeetLite = require('./index.js')

const storage = arg('storage')
const topic = arg('topic')

if (!storage || !topic) {
  process.stderr.write('usage: keet-lite-core --storage <dir> --topic <name>\n')
  process.exit(1)
}

const chat = new KeetLite(storage)

chat.onMessage((message) => {
  const who = b4a.toString(message.sender, 'hex').slice(0, 8)
  process.stdout.write(who + ' ' + message.text + '\n')
})

chat.onPeer((peer) => {
  const who = b4a.toString(peer.remotePublicKey, 'hex').slice(0, 8)
  process.stderr.write('peer ' + who + '\n')
})

let pending = ''

process.stdin.on('data', (data) => {
  pending += b4a.toString(data)
  let nl = pending.indexOf('\n')
  while (nl !== -1) {
    const line = pending.slice(0, nl).replace(/\r$/, '')
    pending = pending.slice(nl + 1)
    nl = pending.indexOf('\n')
    if (line.length === 0) continue
    chat.send(line).catch((err) => {
      process.stderr.write(err.message + '\n')
    })
  }
})

process.once('SIGINT', () => {
  chat.close().then(() => process.exit(0), () => process.exit(1))
})

chat.join(topic).then((info) => {
  process.stderr.write('public key ' + b4a.toString(info.publicKey, 'hex') + '\n')
  process.stderr.write('topic ' + b4a.toString(info.topic, 'hex') + '\n')
  process.stderr.write('type a line to append it to your core\n')
}, (err) => {
  process.stderr.write(err.stack + '\n')
  process.exit(1)
})

function arg (name) {
  const flag = '--' + name
  const index = process.argv.indexOf(flag)
  if (index === -1) return null
  const value = process.argv[index + 1]
  if (!value || value.startsWith('--')) return null
  return value
}
