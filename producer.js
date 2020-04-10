const { Kafka } = require("kafkajs");
const config = require("./config");

const kafka = new Kafka({
  brokers: [config.kafka_server]
});

const producer = kafka.producer();

const message = {
  key1: "value1",
  key2: "value2",
};

// example of header kafka for integration with microservice java that use spring cloudstream
const headers = {
  CID: "1234",
  spring_json_header_types: JSON.stringify({
    CID: "java.lang.String",
    contentType: "java.lang.String"
  })
};

const run = async () => {
  await producer.connect();
  console.log("ready");
  await producer.send({
    topic: config.kafka_topic,
    messages: [
      {
        value: JSON.stringify(message), // must be a string, not object
        headers
      }
    ]
  });
  console.log("sent");
};

run()
  .then(() => {
    console.log("finished");
    process.exit(0);
  })
  .catch(err => console.error(err));
