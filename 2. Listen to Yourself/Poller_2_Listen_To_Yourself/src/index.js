import http from "http";
import dotenv from "dotenv";
import connectDB from "./db/index.js";
import { app } from "./app.js";
import { startPoller } from "./poller/poller.js";
import { startCleanupJob } from "./cleanup/cleanup.js";
import { connectKafka } from "./kafka/kafka.js";
import { initSocket } from "./socket/socket.js";

dotenv.config({
  path: "./.env",
});

const PORT = process.env.PORT || 8003;

const startServer = async () => {
  try {
    await connectDB();
    await connectKafka();
    const server = http.createServer(app);
    initSocket(server);
    startPoller();
    startCleanupJob();
    server.listen(PORT, "0.0.0.0", () => {
      console.log(`APP IS LISTENING ON PORT ${PORT}`);
    });

  } catch (err) {
    console.error("Server startup failed:", err);
    process.exit(1);
  }
};

startServer();