import express from 'express'
import cors from 'cors'

const app=express();
app.use(cors({
    origin: process.env.CORS_ORIGIN === "*" ? true : process.env.CORS_ORIGIN,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
    credentials: true
}));

app.use(express.json());

//Import routes
import orderRouter from './routes/order.routes.js'

//Routes Declaration
app.use('/order/',orderRouter);

export {app}