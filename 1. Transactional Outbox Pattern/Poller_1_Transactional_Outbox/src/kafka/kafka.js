import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "poller-service",
  brokers: ["kafka:29092"], 
});

export const producer = kafka.producer();

export async function connectKafka() {
  try {
    await producer.connect();
    console.log("Kafka Producer Connected");
  } catch (err) {
    console.error("Kafka Connection Failed:", err.message);
    throw err;
  }
}