![GitHub package.json version](https://img.shields.io/github/package-json/v/thzero/library_server_repository_redis)
![David](https://img.shields.io/david/thzero/library_server_repository_redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# library_server_repository_redis

The Redis repository base for [@thzero/library_server](https://github.com/thzero/library_server).

**This package brings no Redis driver of its own.** It handles configuration lookup, connection caching and the mutex around opening a connection, and leaves the driver to a subclass. Use [@thzero/library_server_repository_redis_ioredis](https://github.com/thzero/library_server_repository_redis_ioredis) unless you have a reason to bind a different client.

## Requirements

### NodeJs

[NodeJs](https://nodejs.org) version 22+

### Redis

A reachable Redis server. Nothing in this package starts or provisions one.

### Installation

[![NPM](https://nodei.co/npm/@thzero/library_server_repository_redis.png?compact=true)](https://npmjs.org/package/@thzero/library_server_repository_redis)

```
npm install @thzero/library_server_repository_redis
```

#### Peer dependencies

* `@thzero/library_common`
* `@thzero/library_common_service`
* `@thzero/library_server`

## What it provides

`index.js` — default export `RedisRepository`, extending `Repository` from `@thzero/library_server`.

| Member | Purpose |
|---|---|
| `getClientName()` | The client's name, `'redis'` by default. Override to point at a different `db.*` config block. |
| `_getClient(correlationId, clientName)` | Returns a connected client, opening one on first use. `clientName` is optional and falls back to `_initClientName()`. |
| `_initClientName()` | The name used as the connection cache key. Returns `getClientName()`. |
| `_initializeClient(correlationId, clientName)` | Resolves the connection config, builds the client through the subclass, and caches it. |
| `_initializeClientConnection(correlationId, connectionInfo, clientName, config)` | **Abstract.** Throws `NotImplementedError`. A subclass builds and returns the driver's client here. |

Clients are cached in a **static** map keyed by client name, so every repository instance in the process shares one connection per name. Opening is guarded by a static mutex, so concurrent first calls open exactly one.

### Driver primitives

Everything below is built on two methods a driver subclass implements, so a driver binding gets streams and locks without writing them.

| Member | Purpose |
|---|---|
| `_command(client, command, ...args)` | **Abstract.** Sends one command (`'XADD'`, `'SET'`, ...) and resolves to its reply in the RESP2 shape (arrays, not maps), whatever protocol the connection speaks. |
| `_transaction(client, commands)` | **Abstract.** Sends `[[command, ...args], ...]` in one `MULTI`/`EXEC`; resolves to the replies in order, or rejects with the first command's error. |
| `_execute(correlationId, clientName, command, ...args)` | `_command` on the client `clientName` (`null` for the default). |
| `_ping(correlationId, clientName)` | `PING`. |

### Streams

Every method takes `(correlationId, clientName, ...)`; `clientName` picks the connection (`null` for the default). A blocking read (`blockMs`) holds its connection until it's answered, so give it a client name of its own and send everything else through another. Entries come back as `{ id, fields }`, `fields` an object of the entry's values.

| Member | Purpose |
|---|---|
| `_streamAdd(correlationId, clientName, stream, fields, options)` | Adds an entry (`fields` an object or a flat `[name, value, ...]` array); resolves to its id. `options.maxLength` or `options.minId` trims as it adds, approximately unless `options.approximate` is `false`. |
| `_streamAddMany(correlationId, clientName, stream, entries, options)` | Adds several entries together, in order, in one transaction; resolves to their ids. |
| `_streamTrim(correlationId, clientName, stream, options)` | Trims by `maxLength` or `minId`; resolves to how many were removed. |
| `_streamLastId(correlationId, clientName, stream)` | The newest entry's id, or `'0-0'`. |
| `_streamRead(correlationId, clientName, stream, afterId, options)` | Up to `options.count` (100) entries after `afterId`, waiting up to `options.blockMs` when given. |
| `_streamGroupEnsure(correlationId, clientName, stream, group, startId)` | Makes the consumer group (and the stream), reading from `startId` (`'$'` by default); resolves to `false` when it already exists. |
| `_streamReadGroup(correlationId, clientName, stream, group, consumer, options)` | Up to `options.count` (10) new entries for the consumer, waiting up to `options.blockMs` when given; `options.id` reads its own pending entries instead. |
| `_streamClaimIdle(correlationId, clientName, stream, group, consumer, idleMs, options)` | Takes over entries pending longer than `idleMs` (`XAUTOCLAIM`), leaving out ones deleted meanwhile. |
| `_streamAck(correlationId, clientName, stream, group, ids)` | Acknowledges one id or an array of them. |
| `_streamDeliveries(correlationId, clientName, stream, group, id)` | How many times a pending entry has been delivered; `0` when it isn't pending. |
| `_streamConsumersPrune(correlationId, clientName, stream, group, idleMs)` | Removes consumers with nothing pending, idle longer than `idleMs` (each stopped process leaves one behind); resolves to their names. |

### Locks

A key holding a random token, with an expiry, so a holder that dies can't keep it. Only the token's holder can extend or release it, so a holder whose lock expired can't touch the next holder's.

| Member | Purpose |
|---|---|
| `_lockAcquire(correlationId, clientName, key, ttlMs)` | Resolves to the token, or `null` when someone else holds the lock. |
| `_lockExtend(correlationId, clientName, key, token, ttlMs)` | Resets the expiry if the lock still holds `token`; `false` when it doesn't. |
| `_lockRelease(correlationId, clientName, key, token)` | Deletes the lock if it still holds `token`; `false` when it doesn't. |

## Configuration

The connection block is read from `db.<getClientName()>.connection` and passed verbatim to the subclass — this package never interprets it, so its shape is whatever your driver accepts.

```json
{
    "app": {
        "db": {
            "redis": {
                "connection": {
                    "host": "127.0.0.1",
                    "port": 6379,
                    "username": "<username>",
                    "password": "<password>",
                    "db": 0
                }
            }
        }
    }
}
```

A connection string is equally valid if the driver takes one:

```json
{ "app": { "db": { "redis": { "connection": "redis://127.0.0.1:6379" } } } }
```

A missing or empty `connection` fails at boot with `connectionInfo is empty`, rather than at the first query.

## Wiring it up

Subclass a driver binding rather than this package directly:

```js
import BaseRedisRepository from '@thzero/library_server_repository_redis_ioredis/index.js';

class RegistryRepository extends BaseRedisRepository {
    async publish(correlationId, channel, message) {
        this._enforceNotEmpty('RegistryRepository', 'publish', channel, 'channel', correlationId);

        const client = await this._getClient(correlationId);
        await client.publish(correlationId, channel, JSON.stringify(message));
    }
}
```

A repository over a stream, read by a consumer group on a connection of its own:

```js
class JobsRepository extends BaseRedisRepository {
    async add(correlationId, job) {
        return await this._streamAdd(correlationId, null, 'jobs', { payload: JSON.stringify(job) }, { maxLength: 100000 });
    }

    async next(correlationId, consumer) {
        await this._streamGroupEnsure(correlationId, null, 'jobs', 'workers');
        return await this._streamReadGroup(correlationId, 'jobsReader', 'jobs', 'workers', consumer, { blockMs: 5000 });
    }

    async done(correlationId, ids) {
        await this._streamAck(correlationId, null, 'jobs', 'workers', ids);
    }
}
```

Register it with the injector from `_initRepositories` in your `BootMain` derived class, or from a boot plugin's `initRepositories`.

## Development

```
npm run lint       # eslint .
npm run lint:fix   # eslint . --fix
npm test           # node --test "test/*.test.js"
```
