# keet-lite-core

Two Bare or Node processes join one Hyperswarm topic and replicate one Hypercore per user through Corestore. Chat records are appended to the local core and encoded with [`chat-record-codec`](https://www.npmjs.com/package/chat-record-codec) 1.0.0 (`compact-encoding`).

This package is CommonJS (`require`). Corestore and Hyperswarm are CommonJS, and a Bare worklet loads the same entry with `require`.

## Limits

- One writable Hypercore per process, stored in Corestore under the name `user`. A process only appends to that core.
- Discovery is one shared Hyperswarm topic. It is not a multi-writer log: there is no Autobase view and no single linear history. `history()` reads each core's sequence range and merges the results by timestamp.
- Corestore does not exchange Hypercore keys. On each connection the process sends its core public key in a `keet-lite-core` protomux handshake so the peer can open and download that core.
- There is no UI.

## Record schema

Each Hypercore block is one record. The field order is the schema:

| Field | Encoding | Meaning |
| --- | --- | --- |
| `sender` | `fixed32` | 32-byte public key of the writer's Hypercore |
| `timestamp` | `uint` | milliseconds since the Unix epoch |
| `text` | `string` | UTF-8 text |

`seq` and `key` are not stored in the block. `history()` and message events add them from the core that held the block. `key` is that core's public key. `sender` is the key written into the block; this package writes the local core key there.

```js
const c = require('compact-encoding')
const record = require('chat-record-codec')

const block = c.encode(record, { sender, timestamp, text })
```

`keet-lite-core` re-exports the same codec as `record`.

## API

```js
const KeetLite = require('keet-lite-core')

const chat = new KeetLite(storage, {
  bootstrap // optional HyperDHT bootstrap, used by tests
})

await chat.join(topic) // string name, 64-char hex, or 32-byte buffer
await chat.send(text)
const messages = await chat.history({ key, start, end, limit })

const offMessage = chat.onMessage((message) => {})
const offPeer = chat.onPeer((peer) => {})

await chat.stop() // leave the topic and suspend Hyperswarm
await chat.join(topic) // resume and announce the topic again
await chat.close()
```

`join(topic)` hashes a room name with `hypercore-crypto` to a 32-byte topic. A 32-byte buffer or 64-character hex string is used as the topic itself.

`history(range)` reads a sequence range from one core, or from every known core when `key` is omitted.

- `key` — core public key as a buffer or hex string. Unknown keys return `[]`.
- `start` — inclusive sequence. Default `0`.
- `end` — exclusive sequence. Default is the core length.
- `limit` — if set, keep only the last `limit` sequences inside that window, per core.

The merged list is sorted by `timestamp`, then `seq`. Each item is `{ sender, timestamp, text, seq, key }`. `sender` and `key` are copies of the 32-byte keys.

`onMessage` fires when a new block is appended locally or replicated in. It does not replay blocks that were already stored when the core was opened. Load those with `history()`.

`onPeer` fires when a swarm connection opens. The argument is `{ remotePublicKey }`, the Noise public key of that connection.

Both listeners return an unsubscribe function.

## Resume

`stop()` leaves the topic and calls `swarm.suspend()`. That drops swarm connections and stops announcing. The cores and their blocks stay on disk.

`join()` after `stop()` calls `swarm.resume()` and announces the topic again with `swarm.join`. Peers can find this process on the same topic, and blocks appended while it was stopped replicate once the cores reconnect.

Closing the process and constructing a new `KeetLite` on the same storage does the same announce on `join()`. `history()` then reads the blocks already in the cores.

Suspending a Bare worklet freezes the event loop, so the host should `stop()` during suspend (or let the process exit) and `join()` again on resume. Connections do not survive that gap.

## Run two processes

```sh
npm install

node bin.js --storage /tmp/keet-a --topic lobby
node bin.js --storage /tmp/keet-b --topic lobby
```

Each process prints its Hypercore public key and the topic. Type a line and press enter to append it. The other process prints `sender-prefix text` when the block replicates.

The same entry runs on Bare:

```sh
npx bare bin.js --storage /tmp/keet-a --topic lobby
npx bare bin.js --storage /tmp/keet-b --topic lobby
```

Use a different `--storage` directory for each process. Corestore keeps an exclusive lock on that directory.

## Tests

```sh
npm test
```

`npm test` runs `standard`, then the `brittle` suite on Node (`brittle-node`) and on Bare (`bare`). The swarm tests use a local HyperDHT testnet. They check that two cores on one topic exchange a record, that `history({ start, end, limit })` reads a sequence range, and that `stop()` then `join()` announces the topic again and replicates a block appended while the peer was stopped.

## License

MIT
