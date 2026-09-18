import dotenv from 'dotenv';
import http from 'http';
import { app } from './app.js';
import connectDB from './db/index.js';
import { connectKafka } from "./kafka/kafka.js";
import { startConsumer } from "./kafka/consumerHandler.js";
import { initSocket } from './socket/socket.js';

dotenv.config({
    path: './.env'
});

const startServer = async () => {
    try {
        const server = http.createServer(app);
        initSocket(server);
        await connectDB();
        await connectKafka();
        await startConsumer();
        server.listen(process.env.PORT, '0.0.0.0', () => {
            console.log(`Consumer Service Running On PORT ${process.env.PORT}`);
        });
        
    } catch (error) {
        console.log("Error Starting Consumer Service:", error);
    }
};
startServer();
