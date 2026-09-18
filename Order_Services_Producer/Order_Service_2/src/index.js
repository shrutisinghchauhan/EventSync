import connectDB from "./db/index.js";
import dotenv from 'dotenv';
import http from 'http';
import { app } from './app.js';
import { connectKafka } from "./kafka/kafka.js";
import { startConsumer } from "./kafka/consumerHandler.js";
import { initSocket } from "./socket/socket.js";

dotenv.config({
    path: './.env'
});

const startServer = async () => {
    try 
    {
        await connectDB();
        console.log("Database Connected");
        await connectKafka();
        console.log("Kafka Connected");
        const server = http.createServer(app);
        initSocket(server);
        console.log("Socket.IO Initialized");
        await startConsumer();
        console.log("Kafka Consumer Started");
        server.listen(process.env.PORT, '0.0.0.0', () => {
            console.log(`APP IS LISTENING ON PORT ${process.env.PORT}`);
        });
    } 
    catch (error) 
    {
        console.log("SERVER START ERROR:", error);
    }
};

startServer();