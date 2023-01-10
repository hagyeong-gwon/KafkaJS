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
