const { Kafka } = require("kafkajs");
const config = require("./config");

const kafka = new Kafka({
  brokers: [config.kafka_server]
});

const topic = config.kafka_topic;
const consumer = kafka.consumer({ groupId: `${topic}-group` });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`\n${prefix} ${message.key ? message.key : ' '}#### ${message.value}\n`);
    }
  });
};

run()
  .then(() => console.log(`listening on topic: ${topic}`))
  .catch(err => console.error(err));

// manage exception & traps
const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.map(type => {
  process.on(type, async err => {
    try {
      console.log(`process.on ${type}`);
      console.error(err);
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map(signal => {
  process.once(signal, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, signal);
    }
  });
});
