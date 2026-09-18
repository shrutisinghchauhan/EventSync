import { pool } from "../db/index.js";
import { producer } from "../kafka/kafka.js";
import { getIO } from "../socket/socket.js";

const BATCH_SIZE = process.env.BATCH_SIZE;
const POLL_INTERVAL = process.env.POLL_INTERVAL;

  export async function startPoller() 
  {
    const io = getIO();
    io.emit("poller-started",{timestamp: Date.now(),poller:"Poller_2_ltu"});
    console.log("------------------- Poller started -----------------------------------------------------------------------");
    while (true) 
    {
      const client = await pool.connect();
      try 
      {
            await client.query("BEGIN");
            const res = await client.query(`
                SELECT *
                FROM "Outbox_Listen_To_yourself"
                WHERE status = 'PENDING'
                ORDER BY "createdAt"
                LIMIT $1
                FOR UPDATE SKIP LOCKED`, [BATCH_SIZE]
            );
            io.emit("cycle-started", {timestamp: Date.now(),poller:"Poller_2_ltu"});
            console.log("--------------- CYCLE Started --------------------------------------------\n\n");
            const events = res.rows;
  
            console.log("\n\n");
            for (const event of events) 
            {
                io.emit("locking-and-processing",{timestamp: Date.now(),poller:"Poller_2_ltu",data:{id:event.id,payload:JSON.stringify(event.payload, null, 2)}});
                console.log(` Locking & marking PROCESSING:`);
                console.log("ID:", event.id);
                console.log("Payload:", JSON.stringify(event.payload, null, 2));
                console.log("\n\n");

                await client.query(`
                      UPDATE "Outbox_Listen_To_yourself"
                      SET status = 'PROCESSING', "updatedAt" = NOW()
                      WHERE id = $1
                    `, [event.id]
                );
            }

            await client.query("COMMIT");
            client.release();

            for (const event of events) 
            {
                try 
                {
                  await handleEvent(event);
                } 
                catch (err) 
                {
                    console.log("Unexpected error:", err.message);
                    await pool.query(`
                    UPDATE "Outbox_Listen_To_yourself"
                    SET status = 'PENDING',
                        attempts = attempts + 1,
                        "lastError" = $1,
                        "updatedAt" = NOW()
                    WHERE id = $2
                    `, [err.message, event.id]);
                }
            }

      } 
      catch (err) 
      {
        await client.query("ROLLBACK");
        client.release();
        console.error("TX ERROR:", err);
      }

      await sleep(POLL_INTERVAL);
    }
  }

  async function handleEvent(event) 
  {
    const io = getIO();
    console.log("\n");
    console.log(" Processing:", event.id);
    io.emit("processing",{timestamp: Date.now(),poller:"Poller_2_ltu",data:{id:event.id}})

    const random = Math.random();

    if (random < event.failureRate) 
    {
        io.emit("kafka-failure", {timestamp: Date.now(),poller:"Poller_2_ltu",data:{id:event.id}});
        console.log(" Failure Publishing Event (Simulation) : ",event.id);
        console.log("\n");
        await pool.query(`
          UPDATE "Outbox_Listen_To_yourself"
          SET status = 'PENDING',
              attempts = attempts + 1,
              "updatedAt" = NOW()
          WHERE id = $1
        `, [event.id]);
        return;
    }

    console.log("Success Publishing Event (Simulation) :", event.id);

    try 
    {
      const payload = event.payload;
      payload.id=event.id;
      await producer.send({
        topic: "Orders_2___Listen_To_Yourself_Pattern",
        messages: [
          {
            key: event.id,
            value: JSON.stringify(payload),
          },
        ],
      });
      io.emit("kafka-success", {timestamp: Date.now(),poller:"Poller_2_ltu",data:{id:event.id}});
      console.log("Event sent to Kafka:", event.id);

      await pool.query(`
        UPDATE "Outbox_Listen_To_yourself"
        SET status = 'SENT',
            "processedAt" = NOW(),
            "updatedAt" = NOW()
        WHERE id = $1
      `, [event.id]);

    } 
    catch (err) 
    {
      console.error("Kafka publish failed:", err.message);
      io.emit("kafka-failure", {timestamp: Date.now(),poller:"Poller_2_ltu",data:{id:event.id}});
      await pool.query(`
        UPDATE "Outbox_Listen_To_yourself"
        SET status = 'PENDING',
            attempts = attempts + 1,
            "lastError" = $1,
            "updatedAt" = NOW()
        WHERE id = $2
      `, [err.message, event.id]);
    }
  }

  function sleep(ms) 
  {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
