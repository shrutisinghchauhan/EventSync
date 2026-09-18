import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "poller-service",
  brokers: ["kafka:29092"], 
});

export const consumer = kafka.consumer({
   groupId: "order-service-group"
});

export async function connectKafka() {
  try {
    await consumer.connect();

    await consumer.subscribe({
      topic: "Orders_2___Listen_To_Yourself_Pattern",
      fromBeginning: true,
    });

    console.log("Kafka Consumer Connected");
  } catch (err) {
    console.error("Kafka Connection Failed:", err.message);
    throw err;
  }
}