const { Kafka, logLevel } = require('kafkajs');

function getKafka() {
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  return new Kafka({ clientId: 'payment-poc', brokers, logLevel: logLevel.NOTHING });
}

module.exports = { getKafka };
