const Kafka = require('./kafka');

const producer = new Kafka.Producer();
// consumer retry 2번으로 설정
const consumer = new Kafka.Consumer('test', {retry: 2});

const useProducer = async () => {
  // producer connect
  await producer.init();

  const items = [
    {a: 1},
    {b: 2}
  ];

  // message push
  producer.push(items, 'test');
};

const useConsumer = async () => {
  // consumer connect
  await consumer.init();
  // 메세지 단건 처리
  await consumer.workOne(({value}) => {
    console.log(value);
  });
  /**
   * 메세지 배열로 받기
   * await consumer.work((jobs) => {
   *     jobs.forEach(job => {
   *       const {value} = jobs
   *       console.log(value)
   *     })
   *   });
   */
};

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

// windows 에서는 uncaughtException 이 발생하면 아래 코드의 완료까지 기다려준다.
errorTypes.forEach(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await consumer.consumer.disconnect();
      process.exit(0);
    } catch (e) {
      console.error(type, e.message);
      process.exit(1);
    }
  });
});

// pm2 stop, restart, reload, delete 시에는 SIGINT 가 온다.
signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
