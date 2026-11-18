const b4a = require('b4a')
const c = require('compact-encoding')
const Corestore = require('corestore')
const Hyperswarm = require('hyperswarm')
const Protomux = require('protomux')
const record = require('chat-record-codec')
const toTopic = require('./lib/topic.js')

const PROTOCOL = 'keet-lite-core'

class KeetLite {
  constructor (storage, opts = {}) {
    if (!storage) throw new Error('storage is required')

    this.storage = storage
    this.bootstrap = opts.bootstrap || null

    this.store = null
    this.swarm = null
    this.local = null
    this.topic = null

    this.cores = new Map()
    this._messages = new Set()
    this._peers = new Set()
    this._tracked = new Set()
    this._downloads = []

    this.closed = false
    this._announced = false
    this._opening = null
    this._tail = Promise.resolve()
  }

  get publicKey () {
    return this.local ? this.local.key : null
  }

  join (topic) {
    return this._enqueue(() => this._join(topic))
  }

  send (text) {
    return this._enqueue(() => this._send(text))
  }

  history (range) {
    return this._enqueue(() => this._history(range || {}))
  }

  stop () {
    return this._enqueue(() => this._stop())
  }

  close () {
    return this._enqueue(() => this._close())
  }

  onMessage (fn) {
    this._messages.add(fn)
    return () => this._messages.delete(fn)
  }

  onPeer (fn) {
    this._peers.add(fn)
    return () => this._peers.delete(fn)
  }

  _enqueue (fn) {
    const run = this._tail.then(fn, fn)
    this._tail = run.then(noop, noop)
    return run
  }

  _swarmOpts () {
    if (!this.bootstrap) return {}
    return { bootstrap: this.bootstrap }
  }

  async _open () {
    if (this.local) return
    if (this._opening) return this._opening

    this._opening = this._openNow().catch((err) => {
      this._opening = null
      throw err
    })
    return this._opening
  }

  async _openNow () {
    this.store = new Corestore(this.storage)
    await this.store.ready()

    this.local = this.store.get({ name: 'user' })
    await this.local.ready()
    this._track(this.local)

    // A core is only attached to replication streams while it is downloading.
    // The writable core still needs a live range so peers can read it.
    this._downloads.push(this.local.download({ start: 0, end: -1, linear: true }))
  }

  async _join (topic) {
    if (this.closed) throw new Error('Chat is closed')

    const next = toTopic(topic)
    await this._open()

    if (this.topic && !b4a.equals(this.topic, next) && this._announced && this.swarm && !this.swarm.destroyed) {
      await this.swarm.leave(this.topic)
      this._announced = false
    }
    this.topic = next

    if (!this.swarm || this.swarm.destroyed) {
      this.swarm = new Hyperswarm(this._swarmOpts())
      this.swarm.on('connection', (conn) => this._onConnection(conn))
      this._announced = false
    }

    if (this.swarm.suspended) await this.swarm.resume()

    await this.swarm.listen()

    if (!this._announced) {
      const discovery = this.swarm.join(this.topic, { server: true, client: true })
      await discovery.flushed()
      this._announced = true
    }

    await this.swarm.flush()

    return {
      publicKey: this.local.key,
      topic: this.topic
    }
  }

  async _stop () {
    if (!this.swarm || this.swarm.destroyed) {
      this._announced = false
      return
    }

    if (this.topic && this._announced && !this.swarm.suspended) {
      try {
        await this.swarm.leave(this.topic)
      } catch (err) {
        noop(err)
      }
    }
    this._announced = false

    if (!this.swarm.suspended) await this.swarm.suspend()
  }

  async _send (text) {
    if (this.closed) throw new Error('Chat is closed')
    if (!this.local) throw new Error('Join a topic before sending')
    if (typeof text !== 'string') throw new Error('text must be a string')

    const encoded = c.encode(record, {
      sender: this.local.key,
      timestamp: Date.now(),
      text
    })
    await this.local.append(encoded)
  }

  async _history (range) {
    if (!this.local) throw new Error('Join a topic before reading history')

    const start = range.start == null ? 0 : range.start
    if (!Number.isInteger(start) || start < 0) throw new Error('start must be an integer >= 0')
    if (range.end != null && (!Number.isInteger(range.end) || range.end < start)) {
      throw new Error('end must be an integer >= start')
    }
    if (range.limit != null && (!Number.isInteger(range.limit) || range.limit < 0)) {
      throw new Error('limit must be an integer >= 0')
    }

    const cores = []
    if (range.key != null) {
      const key = toKey(range.key)
      const core = this.cores.get(b4a.toString(key, 'hex'))
      if (!core) return []
      cores.push(core)
    } else {
      for (const core of this.cores.values()) cores.push(core)
    }

    const messages = []
    for (const core of cores) {
      if (!core.opened) await core.ready()
      const from = start
      const to = range.end == null ? core.length : Math.min(core.length, range.end)
      let seq = from
      if (range.limit != null && to - seq > range.limit) seq = to - range.limit

      for (; seq < to; seq++) {
        const raw = await core.get(seq)
        if (!raw) continue
        messages.push(this._message(core, seq, c.decode(record, raw)))
      }
    }

    messages.sort((a, b) => a.timestamp - b.timestamp || a.seq - b.seq)
    return messages
  }

  _onConnection (conn) {
    conn.on('error', noop)
    this.store.replicate(conn)

    const mux = Protomux.from(conn)
    const channel = mux.createChannel({
      protocol: PROTOCOL,
      handshake: c.fixed32,
      onopen: (remoteKey) => {
        this._addRemote(remoteKey).catch(noop)
      }
    })
    if (!channel) return

    channel.open(this.local.key)
    this._emitPeer({ remotePublicKey: copy(conn.remotePublicKey) })
  }

  async _addRemote (remoteKey) {
    if (this.closed || !remoteKey || remoteKey.byteLength !== 32) return
    if (!this.local) return
    if (b4a.equals(remoteKey, this.local.key)) return

    const key = copy(remoteKey)
    const id = b4a.toString(key, 'hex')
    if (this._tracked.has(id)) return

    const core = this.store.get({ key })
    await core.ready()
    if (this.closed || this._tracked.has(id)) return

    this._track(core)
    this._downloads.push(core.download({ start: 0, end: -1, linear: true }))
  }

  _track (core) {
    const id = b4a.toString(core.key, 'hex')
    if (this._tracked.has(id)) return
    this._tracked.add(id)
    this.cores.set(id, core)

    let seq = core.length
    let reading = false

    const pump = () => {
      if (reading || this.closed) return
      reading = true
      this._readNew(core, () => seq, (next) => { seq = next })
        .catch(noop)
        .finally(() => {
          reading = false
          if (!this.closed && seq < core.length) pump()
        })
    }

    core.on('append', pump)
  }

  async _readNew (core, getSeq, setSeq) {
    let seq = getSeq()
    while (seq < core.length && !this.closed) {
      const index = seq
      const raw = await core.get(index)
      if (getSeq() !== index) return
      setSeq(index + 1)
      seq = index + 1
      if (!raw) continue

      let decoded
      try {
        decoded = c.decode(record, raw)
      } catch (err) {
        noop(err)
        continue
      }
      this._emitMessage(this._message(core, index, decoded))
    }
  }

  _message (core, seq, decoded) {
    return {
      sender: copy(decoded.sender),
      timestamp: decoded.timestamp,
      text: decoded.text,
      seq,
      key: copy(core.key)
    }
  }

  _emitMessage (message) {
    for (const fn of this._messages) fn(message)
  }

  _emitPeer (peer) {
    for (const fn of this._peers) fn(peer)
  }

  async _close () {
    if (this.closed) return
    this.closed = true

    for (const download of this._downloads) {
      if (download && typeof download.destroy === 'function') download.destroy()
    }
    this._downloads = []

    if (this.swarm && !this.swarm.destroyed) {
      await this.swarm.destroy()
    }
    this.swarm = null
    this._announced = false

    if (this.store) {
      await this.store.close()
      this.store = null
    }
  }
}

function copy (buf) {
  const out = b4a.alloc(buf.byteLength)
  b4a.copy(buf, out)
  return out
}

function toKey (key) {
  if (b4a.isBuffer(key)) {
    if (key.byteLength !== 32) throw new Error('key must be 32 bytes')
    return key
  }
  if (typeof key === 'string' && /^[0-9a-f]{64}$/i.test(key)) return b4a.from(key, 'hex')
  throw new Error('key must be a 32-byte buffer or hex string')
}

function noop () {}

module.exports = KeetLite
module.exports.record = record
module.exports.toTopic = toTopic
