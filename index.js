import { Mutex as asyncMutex } from 'async-mutex';

import NotImplementedError from '@thzero/library_common/errors/notImplemented.js';

import Repository from '@thzero/library_server/repository/index.js';

class RedisRepository extends Repository {
	static _client = {};
	static _mutexClient = new asyncMutex();
	static _mutexDb = new asyncMutex();
	static _db = {};

	async _getClient(correlationId) {
		return await this._initializeClient(correlationId, this._initClientName());
	}

	get _initClientName() {
		const clientName = this._config.get('db.default');
		return clientName;
	}

	async _initializeClient(correlationId, clientName) {
		this._enforceNotEmpty('RedisRepository', '_initializeClient', 'clientName', clientName, correlationId);

		let client = RedisRepository._client[clientName];
		if (client)
			return client;

		// const release = await this._mutexClient.acquire();
		const release = await RedisRepository._mutexClient.acquire();
		try {
			try {
				const connectionInfo = this._config.get(`db.redis.connection`);
				this._enforceNotEmpty('RedisRepository', '_initializeClient', 'connectionInfo', connectionInfo, correlationId);
				
				client = await this.__initializeClientConnection(connectionInfo, clientName, config);
				this._enforceNotEmpty('RedisRepository', '_initializeClient', 'client', client, correlationId);

				RedisRepository._client[clientName] = client;

				this._enforceNotNull('RedisRepository', '_initializeClient', 'client', client, correlationId);
			}
			catch (err) {
				throw err;
			}
		}
		finally {
			release();
		}

		return client;
	}

	async _initializeClientConnection(correlationId, clientName, config) {
		throw new NotImplementedError();
	}
}

export default RedisRepository;
