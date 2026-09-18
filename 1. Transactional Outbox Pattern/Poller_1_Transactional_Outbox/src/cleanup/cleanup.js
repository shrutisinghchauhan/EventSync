import { pool } from "../db/index.js";
import { getIO } from "../socket/socket.js";

const cleanup_interval=process.env.CLEANUP_INTERVAL

export function startCleanupJob() {
  const io = getIO();
  console.log("------------------- Cleanups scheduled at 5mins cycle -----------------------------------------------------------------------");
  setInterval(async () => 
  {
    try 
    {
      io.emit("cleanup-started",{timestamp: Date.now(),poller:"Poller_1_top"});
      console.log("Running cleanup...");
      const result = await pool.query(`
        DELETE FROM "Outbox_Transactional_Outbox"
        WHERE status = 'SENT'
        AND "processedAt" < NOW() - INTERVAL '5 minutes'
      `);

      console.log(`Deleted ${result.rowCount} SENT events`);

    } catch (err) {
      console.error(" Cleanup failed:", err.message);
    }
  }, cleanup_interval);
}