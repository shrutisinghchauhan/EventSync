import { Server } from "socket.io";

let io;

const initSocket = (server) => 
{
    io = new Server(server, {
        cors: {
            origin: "*",
            methods: ["GET", "POST"]
        }
    });

    io.on("connection", (socket) => {
        console.log("Frontend Connected:", socket.id);
        socket.on("disconnect", () => {
            console.log("Frontend Disconnected");
        });
    });
    return io;
};

const getIO = () => {
    if (!io)throw new Error("Socket Not Initialized");
    return io;
};

export {initSocket,getIO};