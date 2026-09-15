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

Register it with the injector from `_initRepositories` in your `BootMain` derived class, or from a boot plugin's `initRepositories`.

## Development

```
npm run lint       # eslint .
npm run lint:fix   # eslint . --fix
npm test           # node --test "test/*.test.js"
```
