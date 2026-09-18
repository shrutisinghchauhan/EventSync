import { consumer } from "./kafka.js";
import { Order } from "../models/order.model.js";
import { sequelize } from "../db/sequelize.js";
import { ProcessedEvent } from "../models/processedEvent.model.js";
import { getIO } from "../socket/socket.js";

export async function startConsumer() {
 const io = getIO();
  await consumer.run({

    eachMessage: async ({ message }) => {

      const transaction = await sequelize.transaction();

      try {

        const event = JSON.parse(message.value.toString());
        const eventId = event.id;
        console.log("Consumed by:",process.env.SERVICE_NAME);
        console.log("Event : ",event);

        const alreadyProcessed = await ProcessedEvent.findOne({where: {eventId,},transaction,});

        if (alreadyProcessed) 
        {
            console.log("Duplicate event skipped:",eventId);
            await transaction.rollback();
            return;
        }

        const order = await Order.create(
          {
            id: event.id,
            customerName: event.customerName,
            productName: event.productName,
            quantity: event.quantity,
            Pattern_Type: "Listen_To_Yourself"
          },
          { transaction }
        );
        io.emit("order-added", {pattern: "Listen_To_Yourself",order});
        await ProcessedEvent.create({eventId,},{ transaction });
        await transaction.commit();

        console.log("Order inserted successfully:",order.id);
        console.log("\n");
        console.log("\n");

      } catch (err) {

        await transaction.rollback();
        io.emit("order-add-failed", {pattern: "Listen_To_Yourself",order});
        console.error(
          "Consumer transaction failed:",
          err.message
        );
      }
    },
  });
}