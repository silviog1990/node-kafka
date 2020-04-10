module.exports = {
    kafka_server: process.argv[2] || "localhost:9092",
    kafka_topic: process.argv[3] || "topic-example"
};