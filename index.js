import { randomUUID } from 'node:crypto';

import { Mutex as asyncMutex } from 'async-mutex';

import NotImplementedError from '@thzero/library_common/errors/notImplemented.js';

import Repository from '@thzero/library_server/repository/index.js';

// Connection config lookup and caching, plus generic stream and lock commands, for a driver
// subclass to bind. The subclass implements _initializeClientConnection() and the two driver
// primitives, _command() and _transaction(); everything else is built on those.
//
// Streams (_stream*): an entry is returned as { id, fields }, fields an object of its field
// values. A blocking read (blockMs) holds its connection until it's answered, so it wants a client
// name of its own, apart from the one other commands go through.
//
// Locks (_lock*): a key holding a random token, with an expiry, so a holder that dies can't keep
// it. Only the token's holder extends or releases it, so one whose lock expired (and was maybe
// taken by someone else) can't touch the new holder's.
class RedisRepository extends Repository {
	static _client = {};
	static _mutexClient = new asyncMutex();
	static _mutexDb = new asyncMutex();
	static _db = {};
	static ScriptLockExtend = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end';
	static ScriptLockRelease = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end';


	getClientName() {
		return 'redis';
	}
	
	// Sends [command] ('XADD', 'SET', ...) with [args] on [client]; resolves to the reply in its
	// RESP2 shape (arrays, not maps), whatever protocol the connection speaks.
	async _command(client, command, ...args) {
		throw new NotImplementedError();
	}

	// Commands on the client [clientName] (null for the default).
	async _execute(correlationId, clientName, command, ...args) {
		return await this._command(await this._getClient(correlationId, clientName), command, ...args);
	}

	async _getClient(correlationId, clientName) {
		return await this._initializeClient(correlationId, clientName ?? this._initClientName());
	}

	_initClientName() {
		// The client name is only ever a cache key and a label; the connection itself
		// is read from db.<name>.connection below. This used to return the whole
		// db.<name> config block, which stringified to '[object Object]' as the key.
		return this.getClientName();
	}

	async _initializeClient(correlationId, clientName) {
		this._enforceNotEmpty('RedisRepository', '_initializeClient', clientName, 'clientName', correlationId);

		let client = RedisRepository._client[clientName];
		if (client)
			return client;

		// const release = await this._mutexClient.acquire();
		const release = await RedisRepository._mutexClient.acquire();
		try {
			client = RedisRepository._client[clientName];
			if (client)
				return client;

			const connectionInfo = this._config.get(`db.${this.getClientName()}.connection`);
			this._enforceNotEmpty('RedisRepository', '_initializeClient', connectionInfo, 'connectionInfo', correlationId);
			
			client = await this._initializeClientConnection(correlationId, connectionInfo, clientName, this._config);
			this._enforceNotEmpty('RedisRepository', '_initializeClient', client, 'client', correlationId);

			RedisRepository._client[clientName] = client;

			this._enforceNotNull('RedisRepository', '_initializeClient', client, 'client', correlationId);
		}
		finally {
			release();
		}

		return client;
	}

	async _initializeClientConnection(correlationId, connectionInfo, clientName, config) {
		throw new NotImplementedError();
	}

	// Takes the lock [key] for [ttlMs]; resolves to its token, or null when someone else holds it.
	async _lockAcquire(correlationId, clientName, key, ttlMs) {
		const token = randomUUID();
		const result = await this._execute(correlationId, clientName, 'SET', key, token, 'PX', ttlMs, 'NX');
		return result === 'OK' ? token : null;
	}

	// Sets the lock [key]'s expiry to [ttlMs] from now if it still holds [token]; false when it
	// doesn't.
	async _lockExtend(correlationId, clientName, key, token, ttlMs) {
		if (!token)
			return false;
		return Number(await this._execute(correlationId, clientName, 'EVAL', RedisRepository.ScriptLockExtend, 1, key, token, ttlMs)) === 1;
	}

	// Releases the lock [key] if it still holds [token]; false when it doesn't.
	async _lockRelease(correlationId, clientName, key, token) {
		if (!token)
			return false;
		return Number(await this._execute(correlationId, clientName, 'EVAL', RedisRepository.ScriptLockRelease, 1, key, token)) === 1;
	}

	async _ping(correlationId, clientName) {
		return await this._execute(correlationId, clientName, 'PING');
	}

	// Acknowledges [ids] (one or an array) for [group]; resolves to how many were pending.
	async _streamAck(correlationId, clientName, stream, group, ids) {
		const list = Array.isArray(ids) ? ids : [ ids ];
		if (list.length === 0)
			return 0;
		return await this._execute(correlationId, clientName, 'XACK', stream, group, ...list);
	}

	// Adds an entry with [fields] (an object, or a flat [name, value, ...] array) to [stream];
	// resolves to its id. [options]: maxLength or minId trims the stream as it's added to
	// (approximate, the default, lets Redis trim whole nodes; much cheaper).
	async _streamAdd(correlationId, clientName, stream, fields, options) {
		return await this._execute(correlationId, clientName, 'XADD', stream, ...this._streamAddArgs(correlationId, fields, options));
	}

	_streamAddArgs(correlationId, fields, options) {
		const flat = Array.isArray(fields) ? fields : Object.entries(fields ?? {}).flat();
		this._enforce('RedisRepository', '_streamAdd', flat.length > 0 && (flat.length % 2 === 0), 'fields', correlationId, 'An entry needs name and value pairs.');
		return [ ...this._streamTrimArgs(options), '*', ...flat ];
	}

	// Adds [entries] (each fields, as for _streamAdd) to [stream] together, in order, in one
	// transaction; resolves to their ids.
	async _streamAddMany(correlationId, clientName, stream, entries, options) {
		if (!entries || (entries.length === 0))
			return [];
		const client = await this._getClient(correlationId, clientName);
		return await this._transaction(client, entries.map(fields => [ 'XADD', stream, ...this._streamAddArgs(correlationId, fields, options) ]));
	}

	// Takes over for [consumer] up to [options] count (default 10) entries of [group] pending
	// longer than [idleMs] (their consumer stopped, or couldn't finish them). Entries deleted from
	// the stream meanwhile are left out.
	async _streamClaimIdle(correlationId, clientName, stream, group, consumer, idleMs, options) {
		const result = await this._execute(correlationId, clientName, 'XAUTOCLAIM', stream, group, consumer, idleMs, options?.start ?? '0-0', 'COUNT', options?.count ?? 10);
		return this._streamEntries(result?.[1]);
	}

	// Removes the consumers of [group] that have nothing pending and have been idle longer than
	// [idleMs] (each process that stopped leaves one behind); resolves to their names.
	async _streamConsumersPrune(correlationId, clientName, stream, group, idleMs) {
		const consumers = await this._execute(correlationId, clientName, 'XINFO', 'CONSUMERS', stream, group);
		const removed = [];
		for (const consumer of (consumers ?? [])) {
			const info = this._streamFields(consumer);
			if ((Number(info.pending) === 0) && (Number(info.idle) > idleMs)) {
				await this._execute(correlationId, clientName, 'XGROUP', 'DELCONSUMER', stream, group, info.name);
				removed.push(info.name);
			}
		}
		return removed;
	}

	// How many times the pending entry [id] has been delivered in [group]; 0 when it isn't pending.
	async _streamDeliveries(correlationId, clientName, stream, group, id) {
		const pending = await this._execute(correlationId, clientName, 'XPENDING', stream, group, id, id, 1);
		return Number(pending?.[0]?.[3] ?? 0);
	}

	_streamEntries(raw) {
		return (raw ?? []).filter(([ , fields ]) => !!fields).map(([ id, fields ]) => ({ id, fields: this._streamFields(fields) }));
	}

	// An XREAD / XREADGROUP reply ([[stream, [[id, fields], ...]], ...], or null) as entries.
	_streamEntriesFromRead(response) {
		const entries = [];
		for (const [ , streamEntries ] of (response ?? []))
			entries.push(...this._streamEntries(streamEntries));
		return entries;
	}

	// A flat [name, value, ...] reply as an object.
	_streamFields(flat) {
		const fields = {};
		for (let i = 0; i < (flat?.length ?? 0) - 1; i += 2)
			fields[flat[i]] = flat[i + 1];
		return fields;
	}

	// Makes the consumer [group] on [stream] (and the stream, when there isn't one), reading from
	// [startId] ('$', the default: only what's added from now on; '0': everything). Resolves to
	// true when it made it, false when it was already there.
	async _streamGroupEnsure(correlationId, clientName, stream, group, startId) {
		try {
			await this._execute(correlationId, clientName, 'XGROUP', 'CREATE', stream, group, startId ?? '$', 'MKSTREAM');
			return true;
		}
		catch (err) {
			if (String(err?.message).includes('BUSYGROUP'))
				return false;
			throw err;
		}
	}

	// The id of [stream]'s newest entry, or '0-0' when it's empty or doesn't exist.
	async _streamLastId(correlationId, clientName, stream) {
		const entries = await this._execute(correlationId, clientName, 'XREVRANGE', stream, '+', '-', 'COUNT', 1);
		return entries?.[0]?.[0] ?? '0-0';
	}

	// Up to [options] count (default 100) entries of [stream] after [afterId], waiting up to
	// blockMs for some when given.
	async _streamRead(correlationId, clientName, stream, afterId, options) {
		const args = [ 'COUNT', options?.count ?? 100 ];
		if (options?.blockMs !== undefined && options?.blockMs !== null)
			args.push('BLOCK', options.blockMs);
		const response = await this._execute(correlationId, clientName, 'XREAD', ...args, 'STREAMS', stream, afterId);
		return this._streamEntriesFromRead(response);
	}

	// Up to [options] count (default 10) entries of [stream] for [consumer] of [group]: new ones
	// ('>', the default id), waiting up to blockMs for some when given; or, with an id, the
	// consumer's own pending ones after it.
	async _streamReadGroup(correlationId, clientName, stream, group, consumer, options) {
		const args = [ 'GROUP', group, consumer, 'COUNT', options?.count ?? 10 ];
		if (options?.blockMs !== undefined && options?.blockMs !== null)
			args.push('BLOCK', options.blockMs);
		const response = await this._execute(correlationId, clientName, 'XREADGROUP', ...args, 'STREAMS', stream, options?.id ?? '>');
		return this._streamEntriesFromRead(response);
	}

	// Trims [stream] to [options] maxLength entries, or to entries from minId on; resolves to how
	// many were removed.
	async _streamTrim(correlationId, clientName, stream, options) {
		const trim = this._streamTrimArgs(options);
		if (trim.length === 0)
			return 0;
		return await this._execute(correlationId, clientName, 'XTRIM', stream, ...trim);
	}

	_streamTrimArgs(options) {
		const approximate = options?.approximate === false ? [] : [ '~' ];
		if (options?.minId !== undefined && options?.minId !== null)
			return [ 'MINID', ...approximate, options.minId ];
		if (options?.maxLength !== undefined && options?.maxLength !== null)
			return [ 'MAXLEN', ...approximate, options.maxLength ];
		return [];
	}

	// Sends [commands] ([[command, ...args], ...]) on [client] in one MULTI/EXEC; resolves to their
	// replies in order, or rejects with the first command's error.
	async _transaction(client, commands) {
		throw new NotImplementedError();
	}
}

export default RedisRepository;
