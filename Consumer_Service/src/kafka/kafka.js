import { Kafka } from "kafkajs";

const TOPICS = [
    "Orders_1___Transactional_Outbox_Pattern",
    "Orders_2___Listen_To_Yourself_Pattern",
    "Orders_3___Transactional_Log_Tailing"
];

const kafka = new Kafka({
  clientId: "poller-service",
  brokers: ["kafka:29092"], 
});

export const consumer = kafka.consumer({
   groupId: "consumer-service-group"
});

export async function connectKafka() {
  try {
    await consumer.connect();

    for (const topic of TOPICS) 
    {
        await consumer.subscribe({topic,fromBeginning: true});
        console.log(`Subscribed To Topic: ${topic}`);
    }

    console.log("Kafka Consumer Connected");
  } catch (err) {
    console.error("Kafka Connection Failed:", err.message);
    throw err;
  }
}