const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const brokers = ''; // kafka 서버

const kafka = new Kafka({
	clientId: 'test',
	brokers,
	// logLevel: logLevel.DEBUG
});

class Consumer {
	/**
	 * Kafka는 순서는 하나의 partition내에만 보장하며 자유로운 scale out이 어렵기 때문에
	 * 처음부터 process(instance) 개수가 정해진 특수 목적 queue를 사용할 때 적합하다.
	 *
	 * 같은 group Id를 가진 consumer를 같은 topic을 구독하려고 할 때 consumer는 partition 개수만큼만
	 * 생성 할 수 있다. (1:1 구조가 적합하다.)
	 *
	 * - fromBeginning
	 *  - true: 가장 오래된 오프셋부터
	 *  - false: 가장 최신 오프셋부터
	 * - consumerOption.sessionTimeout
	 *  - 실패를 감지하는데 사용하는 시간제한으로 만료 전 consumer에서 heartbeat를 수신하지 않을 경우 broker에서 재조정을 시작한다. (rebalaance)
	 * - consumerOption.heartbeatInterval
	 *  - consumer에서 broker로 보내는 heartbeat를 얼마나 자주 보낼 건지 조정한다. 일반적으로 sessionTimeout의 1/3 정도로 설정한다.
	 *
	 * @param {string} topic
	 * @param {number} retry 오류 topic으로 보내기 전 retry 수
	 * @param {boolean} fromBeginning 오프셋이 유효하지 않거나 정의되지 않은 경우의 동작을 정의한다.
	 * @param {object} consumerOption
	 * @param {number} consumerOption.sessionTimeout 실패를 감지하는데 사용하는 시간제한 (ms)
	 * @param {string} consumerOption.groupId
	 */
	constructor(topic, {retry = 1, fromBeginning = false, consumerOption = {}, errorTopic} = {}) {
		const groupId = consumerOption.groupId || topic;

		this.topic = topic;
		this.errorTopic = errorTopic || `${this.topic}-error`;
		this.retry = retry;
		this.fromBeginning = fromBeginning;
		this.connect = false;
		this.consumer = kafka.consumer({...consumerOption, groupId});
		this.initPrm = new Promise(resolve=>{
			this.initResolve = resolve;
		});
	}
	async init() {
		if (this.connect) return;
		if (this.connect == null) return this.initPrm; // init 중일 때 재호출 된 경우
		this.connect = null;

		await this.consumer.connect();
		await this.consumer.subscribe({ topic: this.topic, fromBeginning: this.fromBeginning});

		// this.producer = kafka.producer();
		// await this.producer.connect();
		this.producer = new Producer();
		await this.producer.init();

		this.connect = true;
		this.initResolve();
	}
	async workOne(f) {
		await this.consumer.run({
			// eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
			eachMessage: async ({message}) => {
				try {
					await f({...message, value: JSON.parse(message.value)});
				} catch (e) {
					await this.onError([message], e);
				}
			}
		});
	}
	async work(f) {
		await this.consumer.run({
			eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
        isStale,
        pause,
      }) => {
				if (!isRunning() || isStale()) throw new Error('pause');

				try {
					const jobs = batch.messages.map(message => {
						return {...message, value: JSON.parse(message.value)};
					});
					await f(jobs);
				} catch (e) {
					await this.onError(batch.messages, e);
				}
			}
		});
	}
	async onError(messages, error) {
		try {
			const retryTarget = [];
			const errorTarget = [];

			for (const message of messages) {
				const {value, key, headers} = message;
				if (+(message.headers.retry || 0) >= this.retry) {
					message.headers.errorMessage = error.toString();
					this.producer.makeMessages(value, {headers, key, messages: errorTarget});
				} else {
					// kafka로 메세지를 발행할 때 headers value는 string이어야 한다.
					message.headers.retry = `${+(message.headers.retry || 0) + 1}`;
					this.producer.makeMessages(value, {headers, key, messages: retryTarget});
				}
			}

			retryTarget && this.producer.push(retryTarget, this.topic, {isMakeMsg: false});
			errorTarget && this.producer.push(errorTarget, this.errorTopic, {isMakeMsg: false});

		} catch (e) {
			throw new Error(e);
		}
	}
}

class Producer {
	constructor({producerOption = {}} = {}) {
		this.connect = false;
		this.producer = kafka.producer(producerOption);

		this.initPrm = new Promise(resolve=>{
			this.initResolve = resolve;
		});
	}
	async init() {
		if (this.connect) return;
		if (this.connect == null) return this.initPrm; // init 중일 때 재호출 된 경우
		this.connect = null;

		await this.producer.connect();

		this.connect = true;
		this.initResolve();
	}
	makeMessages(items, {headers = {}, key = null, messages = []} = {}) {
		// 변수 타입 확인
		const type = Object.prototype.toString.call(items);
		if (type === '[object Array]') {
			items.forEach(item => {
				messages.push({
					key: key,
					headers: headers,
					value: JSON.stringify(item)
				});
			});
		} else if (type === '[object Object]') {
			messages.push({
				key: key,
				headers: headers,
				value: JSON.stringify(items)
			});
		} else {
			messages.push({
				key: key,
				headers: headers,
				value: items.toString()
			});
		}

		return messages;
	}

	/**
	 * kafka 메세지를 발행한다.
	 *
	 * @param items 보낼 데이터 / 메세지
	 * @param {string} topic
	 * @param headers 메세지 당 넣을 headers (isMakeMsg = true 때 사용)
	 * @param key 메세지 당 넣을 key (isMakeMsg = true 때 사용)
	 * @param {boolean} isMakeMsg message를 형식에 맞게 만들어 줄건지
	 */
	push(items, topic, {headers, key, isMakeMsg = true} = {}) {
		const messages = isMakeMsg ? this.makeMessages(items, {headers, key}) : items;
		this.producer.send({
			topic,
			messages,
			compression: CompressionTypes.GZIP
		});
	}
}

module.exports = {
	Consumer,
	Producer
};
