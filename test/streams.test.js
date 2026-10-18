import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import '@thzero/library_common/utility/string.js';
import RedisRepository from '../index.js';

const inject = (target, name, value) => {
	Object.defineProperty(target, name, { value, writable: true, configurable: true });
	return target;
};

const newLogger = () => ({ debug() {}, info() {}, warn() {}, error() {}, exception() {}, fatal() {}, trace() {} });

// A driver that records what it's sent and answers from [replies] (by command, a value or a
// function of the arguments).
class TestRedisRepository extends RedisRepository {
	constructor(replies = {}) {
		super();
		this.sent = [];
		this.replies = replies;
	}
	async _initializeClientConnection(correlationId, connectionInfo, clientName) {
		return { name: clientName };
	}
	async _command(client, command, ...args) {
		this.sent.push({ client: client.name, command, args });
		const reply = this.replies[command];
		return typeof reply === 'function' ? reply(...args) : (reply ?? null);
	}
	async _transaction(client, commands) {
		this.sent.push({ client: client.name, transaction: commands });
		return commands.map((_, i) => `${i + 1}-0`);
	}
}

const newRepository = (replies) => {
	const repository = new TestRedisRepository(replies);
	inject(repository, '_logger', newLogger());
	inject(repository, '_config', { get: (path) => path === 'db.redis.connection' ? { host: 'h', port: 6379 } : null });
	return repository;
};

beforeEach(() => {
	// the client cache is static, so it leaks between tests
	RedisRepository._client = {};
});

describe('driver primitives', () => {
	it('are not implemented on the base', async () => {
		const base = new RedisRepository();
		await assert.rejects(() => base._command({}, 'PING'), /NotImplemented|not implemented/i);
		await assert.rejects(() => base._transaction({}, []), /NotImplemented|not implemented/i);
	});

	it('_execute sends on the named client, or the default', async () => {
		const repository = newRepository({ PING: 'PONG' });
		assert.equal(await repository._ping('cid', null), 'PONG');
		await repository._execute('cid', 'reader', 'PING');
		assert.deepEqual(repository.sent.map(s => s.client), [ 'redis', 'reader' ]);
	});
});

describe('_streamAdd', () => {
	it('adds an object as name and value pairs', async () => {
		const repository = newRepository({ XADD: '1-0' });
		assert.equal(await repository._streamAdd('cid', null, 's', { topic: 't', payload: '{}' }), '1-0');
		assert.deepEqual(repository.sent[0].args, [ 's', '*', 'topic', 't', 'payload', '{}' ]);
	});

	it('takes a flat array as it is', async () => {
		const repository = newRepository();
		await repository._streamAdd('cid', null, 's', [ 'a', '1' ]);
		assert.deepEqual(repository.sent[0].args, [ 's', '*', 'a', '1' ]);
	});

	it('trims approximately by length or age', async () => {
		const repository = newRepository();
		await repository._streamAdd('cid', null, 's', { a: 1 }, { maxLength: 50 });
		await repository._streamAdd('cid', null, 's', { a: 1 }, { minId: '100-0' });
		await repository._streamAdd('cid', null, 's', { a: 1 }, { maxLength: 50, approximate: false });
		assert.deepEqual(repository.sent.map(s => s.args.slice(1, -3)), [ [ 'MAXLEN', '~', 50 ], [ 'MINID', '~', '100-0' ], [ 'MAXLEN', 50 ] ]);
	});

	it('refuses an entry without pairs', async () => {
		const repository = newRepository();
		await assert.rejects(() => repository._streamAdd('cid', null, 's', {}), /pairs/);
		await assert.rejects(() => repository._streamAdd('cid', null, 's', [ 'a' ]), /pairs/);
	});

	it('_streamAddMany adds them in one transaction', async () => {
		const repository = newRepository();
		assert.deepEqual(await repository._streamAddMany('cid', null, 's', [ { a: 1 }, { b: 2 } ], { maxLength: 9 }), [ '1-0', '2-0' ]);
		assert.deepEqual(repository.sent[0].transaction, [ [ 'XADD', 's', 'MAXLEN', '~', 9, '*', 'a', 1 ], [ 'XADD', 's', 'MAXLEN', '~', 9, '*', 'b', 2 ] ]);
		assert.deepEqual(await repository._streamAddMany('cid', null, 's', []), []);
		assert.equal(repository.sent.length, 1);
	});
});

describe('_streamTrim', () => {
	it('trims, or does nothing without a limit', async () => {
		const repository = newRepository({ XTRIM: 3 });
		assert.equal(await repository._streamTrim('cid', null, 's', { maxLength: 10 }), 3);
		assert.equal(await repository._streamTrim('cid', null, 's', {}), 0);
		assert.deepEqual(repository.sent.map(s => s.args), [ [ 's', 'MAXLEN', '~', 10 ] ]);
	});
});

describe('reading', () => {
	const reply = [ [ 's', [ [ '1-0', [ 'topic', 't', 'payload', 'p' ] ], [ '2-0', [ 'payload', 'q' ] ] ] ] ];

	it('_streamRead reads after an id, blocking when asked', async () => {
		const repository = newRepository({ XREAD: reply });
		const entries = await repository._streamRead('cid', 'reader', 's', '0-0', { count: 5, blockMs: 100 });
		assert.deepEqual(repository.sent[0].args, [ 'COUNT', 5, 'BLOCK', 100, 'STREAMS', 's', '0-0' ]);
		assert.deepEqual(entries, [ { id: '1-0', fields: { topic: 't', payload: 'p' } }, { id: '2-0', fields: { payload: 'q' } } ]);
	});

	it('_streamRead with nothing new is empty', async () => {
		const repository = newRepository({ XREAD: null });
		assert.deepEqual(await repository._streamRead('cid', null, 's', '0-0'), []);
		assert.deepEqual(repository.sent[0].args, [ 'COUNT', 100, 'STREAMS', 's', '0-0' ]);
	});

	it('_streamReadGroup reads new entries for a consumer', async () => {
		const repository = newRepository({ XREADGROUP: reply });
		const entries = await repository._streamReadGroup('cid', null, 's', 'g', 'c', { blockMs: 5 });
		assert.deepEqual(repository.sent[0].args, [ 'GROUP', 'g', 'c', 'COUNT', 10, 'BLOCK', 5, 'STREAMS', 's', '>' ]);
		assert.equal(entries.length, 2);
	});

	it('_streamLastId is the newest id, or 0-0', async () => {
		assert.equal(await newRepository({ XREVRANGE: [ [ '9-0', [] ] ] })._streamLastId('cid', null, 's'), '9-0');
		assert.equal(await newRepository({ XREVRANGE: [] })._streamLastId('cid', null, 's'), '0-0');
	});
});

describe('consumer groups', () => {
	it('_streamGroupEnsure makes the group and stream, or finds it', async () => {
		const repository = newRepository();
		assert.equal(await repository._streamGroupEnsure('cid', null, 's', 'g'), true);
		assert.deepEqual(repository.sent[0].args, [ 'CREATE', 's', 'g', '$', 'MKSTREAM' ]);

		const existing = newRepository({ XGROUP: () => { throw new Error('BUSYGROUP Consumer Group name already exists'); } });
		assert.equal(await existing._streamGroupEnsure('cid', null, 's', 'g', '0'), false);
	});

	it('_streamGroupEnsure passes other errors on', async () => {
		const repository = newRepository({ XGROUP: () => { throw new Error('WRONGTYPE'); } });
		await assert.rejects(() => repository._streamGroupEnsure('cid', null, 's', 'g'), /WRONGTYPE/);
	});

	it('_streamClaimIdle leaves out entries deleted meanwhile', async () => {
		const repository = newRepository({ XAUTOCLAIM: [ '0-0', [ [ '1-0', [ 'a', '1' ] ], [ '2-0', null ] ], [] ] });
		assert.deepEqual(await repository._streamClaimIdle('cid', null, 's', 'g', 'c', 1000), [ { id: '1-0', fields: { a: '1' } } ]);
		assert.deepEqual(repository.sent[0].args, [ 's', 'g', 'c', 1000, '0-0', 'COUNT', 10 ]);
	});

	it('_streamAck acknowledges one or many, nothing for none', async () => {
		const repository = newRepository({ XACK: (s, g, ...ids) => ids.length });
		assert.equal(await repository._streamAck('cid', null, 's', 'g', '1-0'), 1);
		assert.equal(await repository._streamAck('cid', null, 's', 'g', [ '1-0', '2-0' ]), 2);
		assert.equal(await repository._streamAck('cid', null, 's', 'g', []), 0);
		assert.equal(repository.sent.length, 2);
	});

	it('_streamDeliveries counts deliveries of a pending entry', async () => {
		assert.equal(await newRepository({ XPENDING: [ [ '1-0', 'c', 10, 3 ] ] })._streamDeliveries('cid', null, 's', 'g', '1-0'), 3);
		assert.equal(await newRepository({ XPENDING: [] })._streamDeliveries('cid', null, 's', 'g', '1-0'), 0);
	});

	it('_streamConsumersPrune removes idle consumers with nothing pending', async () => {
		const repository = newRepository({ XINFO: [
			[ 'name', 'busy', 'pending', 2, 'idle', 999999 ],
			[ 'name', 'gone', 'pending', 0, 'idle', 999999 ],
			[ 'name', 'recent', 'pending', 0, 'idle', 10 ]
		] });
		assert.deepEqual(await repository._streamConsumersPrune('cid', null, 's', 'g', 60000), [ 'gone' ]);
		assert.deepEqual(repository.sent[1].args, [ 'DELCONSUMER', 's', 'g', 'gone' ]);
	});
});

describe('locks', () => {
	it('_lockAcquire returns a token, or null when held', async () => {
		const repository = newRepository({ SET: 'OK' });
		const token = await repository._lockAcquire('cid', null, 'k', 500);
		assert.ok(token);
		assert.deepEqual(repository.sent[0].args, [ 'k', token, 'PX', 500, 'NX' ]);
		assert.equal(await newRepository({ SET: null })._lockAcquire('cid', null, 'k', 500), null);
	});

	it('_lockExtend and _lockRelease only act with the token', async () => {
		const repository = newRepository({ EVAL: 1 });
		assert.equal(await repository._lockExtend('cid', null, 'k', 't', 500), true);
		assert.equal(await repository._lockRelease('cid', null, 'k', 't'), true);
		assert.equal(await repository._lockRelease('cid', null, 'k', null), false);
		assert.deepEqual(repository.sent.map(s => s.args.slice(1)), [ [ 1, 'k', 't', 500 ], [ 1, 'k', 't' ] ]);
		assert.equal(await newRepository({ EVAL: 0 })._lockExtend('cid', null, 'k', 't', 500), false);
	});
});
