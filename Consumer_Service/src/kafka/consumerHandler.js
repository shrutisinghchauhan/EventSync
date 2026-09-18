import { consumer } from "./kafka.js";
import { sequelize } from "../db/sequelize.js";
import { EventsConsumed } from "../models/eventsConsumed.model.js";
import { ProcessedEvent } from "../models/processedEvent.model.js";
import { getIO } from "../socket/socket.js";

const extractPayload = (topic, event) => {
  if (topic === "Orders_3___Transactional_Log_Tailing") {
    return event?.payload?.after;
  }

  return event;
};

const extractOrderId = (payload, event, message) => {
  return (
    payload?.order_id ||
    payload?.orderId ||
    payload?.id ||
    event?.order_id ||
    event?.orderId ||
    event?.id ||
    message.key?.toString()
  );
};

const startConsumer = async () => {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => 
    {
      const transaction = await sequelize.transaction();
      let transactionFinished = false;

      try 
      {
        const io = getIO();
        const event = JSON.parse(message.value.toString());
        const payload = extractPayload(topic, event);
        const order_id = extractOrderId(payload, event, message);

        if (!order_id) {
          throw new Error("Unable to resolve order_id from consumed event");
        }

        const alreadyProcessed = await ProcessedEvent.findOne({
          where: { order_id },
          transaction,
        });

        if (alreadyProcessed) {
          await transaction.rollback();
          transactionFinished = true;
          console.log("Duplicate event skipped:", order_id);
          io.emit("consumer-duplicate-event", { topic, payload, order_id, timestamp: Date.now() });
          return;
        }

        await EventsConsumed.create(
          {
            order_id,
            topic,
            partition,
            offset: message.offset,
            order_event: payload,
          },
          { transaction }
        );

        await ProcessedEvent.create({ order_id }, { transaction });
        await transaction.commit();
        transactionFinished = true;

        console.log(`Event Received From ${topic}`, payload);
        io.emit("consumer-event", { topic, payload, order_id, timestamp: Date.now() });
      } 
      catch (error) 
      {
        if (!transactionFinished) {
          await transaction.rollback();
        }
        console.log("Consumer Error:", error);
      }
    }
  });
};

export { startConsumer };
