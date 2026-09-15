import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import '@thzero/library_common/utility/string.js';
import RedisRepository from '../index.js';

const inject = (target, name, value) => {
	Object.defineProperty(target, name, { value, writable: true, configurable: true });
	return target;
};

const newLogger = () => ({ debug() {}, info() {}, warn() {}, error() {}, exception() {}, fatal() {}, trace() {} });

const newConfig = (tree) => ({
	get(path, fallback = null) {
		let node = tree;
		for (const part of path.split('.')) {
			if (node === null || node === undefined)
				return fallback;
			node = node[part];
		}
		return node === undefined ? fallback : node;
	}
});

// The base leaves the actual connection to a subclass.
class TestRedisRepository extends RedisRepository {
	constructor() {
		super();
		this.connections = [];
	}
	async _initializeClientConnection(correlationId, connectionInfo, clientName) {
		this.connections.push({ connectionInfo, clientName });
		return { client: clientName };
	}
}

let repository;

beforeEach(() => {
	// the client cache is static, so it leaks between tests
	RedisRepository._client = {};

	repository = new TestRedisRepository();
	inject(repository, '_logger', newLogger());
	inject(repository, '_config', newConfig({ db: { redis: { connection: { host: 'h', port: 6379 } } } }));
});

describe('getClientName', () => {
	it('is redis', () => {
		assert.equal(repository.getClientName(), 'redis');
	});
});

describe('_initializeClient', () => {
	it('requires a client name', async () => {
		await assert.rejects(() => repository._initializeClient('cid', null), /clientName is empty/);
		await assert.rejects(() => repository._initializeClient('cid', ''), /clientName is empty/);
	});

	it('builds the connection from db.<name>.connection', async () => {
		await repository._initializeClient('cid', 'redis');
		assert.deepEqual(repository.connections[0].connectionInfo, { host: 'h', port: 6379 });
		assert.equal(repository.connections[0].clientName, 'redis');
	});

	it('caches the client', async () => {
		const first = await repository._initializeClient('cid', 'redis');
		const second = await repository._initializeClient('cid', 'redis');
		assert.equal(second, first);
		assert.equal(repository.connections.length, 1, 'the second call came from the cache');
	});

	it('rejects when no connection is configured', async () => {
		inject(repository, '_config', newConfig({ db: { redis: {} } }));
		await assert.rejects(() => repository._initializeClient('cid', 'redis'), /connectionInfo is empty/);
	});

	// Regression: the body sat inside a `try { ... } catch (err) { throw err; }`
	// wrapper that did nothing but obscure the flow. The error still has to escape
	// past the mutex release.
	it('lets a connection failure escape, and still releases the mutex', async () => {
		repository._initializeClientConnection = async () => { throw new Error('boom'); };
		await assert.rejects(() => repository._initializeClient('cid', 'redis'), /boom/);
		// a second call proceeds, which it could not do if release() had been skipped
		await assert.rejects(() => repository._initializeClient('cid', 'redis'), /boom/);
	});

	it('rejects when the subclass hands back nothing', async () => {
		repository._initializeClientConnection = async () => null;
		await assert.rejects(() => repository._initializeClient('cid', 'redis'), /client is empty/);
	});
});

describe('_initializeClientConnection', () => {
	it('is not implemented on the base', async () => {
		const base = new RedisRepository();
		inject(base, '_logger', newLogger());
		await assert.rejects(() => base._initializeClientConnection('cid', {}, 'redis', null), /NotImplemented|not implemented/i);
	});
});

describe('_initClientName', () => {
	// Regression: this returned this._config.get('db.redis') - the whole config
	// block rather than a name - and that object became the key of the static
	// client cache, stringifying to '[object Object]'. Two named clients would
	// have collided on that one key.
	it('returns a name, not the config block', () => {
		assert.equal(repository._initClientName(), 'redis');
	});

	it('keys the client cache by that name', async () => {
		await repository._getClient('cid', null);
		assert.deepEqual(Object.keys(RedisRepository._client), [ 'redis' ]);
	});

	it('_getClient falls back to it when given no client name', async () => {
		await repository._getClient('cid', null);
		assert.equal(repository.connections.length, 1);
	});

	it('_getClient uses the name it is given', async () => {
		await repository._getClient('cid', 'explicit');
		assert.equal(repository.connections[0].clientName, 'explicit');
	});
});
