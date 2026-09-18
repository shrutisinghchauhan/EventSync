import { sequelize } from "../db/sequelize.js";
import { Order } from "../models/order.model.js";
import { Outbox} from "../models/outbox.model.js";
import {Outbox_Listen_To_yourself} from "../models/outbox_ltu.js"
import { randomUUID } from "crypto";
import { getIO } from "../socket/socket.js"

export const addOrders_Transactional_Outbox_Pattern = async (req, res) => {
  const { orders,failureRate} = req.body;
  const transaction = await sequelize.transaction();
  const io = getIO();
  try {
    const result = [];
    for (let order of orders) 
    {
      const id = randomUUID();
      const newOrder = await Order.create(
        {
          id,
          customerName: order.customerName,
          productName: order.productName,
          quantity: order.quantity,
          Pattern_Type: "Transactional_Outbox"
        },
        { transaction }
      );
      io.emit("order-added", {pattern: "Transactional_Outbox",newOrder});

      const tp=await Outbox.create(
        {
          id: randomUUID(),
          payload: order,
          status: "PENDING",
          failureRate:req.body.failureRate
        },
        { transaction }
      );
      io.emit("outbox-order-added", {pattern: "Transactional_Outbox",tp});
      result.push(newOrder);
    }

    await transaction.commit();
    res.status(201).json(result);
  } 
  catch (err) 
  {
    await transaction.rollback();
    io.emit("order-add-failed", {pattern: "Transactional_Outbox",order});
    res.status(500).json({ error: err.message });
  }
};


export const addOrders_Listen_To_Yourself = async (req, res) => {
  const { orders,failureRate} = req.body;
  const io = getIO();
  const transaction = await sequelize.transaction();

  try {
    const result = [];

    for (let order of orders) {
      const ev= await Outbox_Listen_To_yourself.create(
        {
          id: randomUUID(),
          payload: order,
          status: "PENDING",
          failureRate:req.body.failureRate
        },
        { transaction }
      );
      io.emit("outbox-order-added", {pattern: "Listen_To_Yourself",ev});
      result.push(ev);
    }

    await transaction.commit();

    res.status(201).json(result);
  } catch (err) {
    await transaction.rollback();
    res.status(500).json({ error: err.message });
  }
};

export const addOrders_Transactional_Log_Tailing_Pattern = async (req, res) => {
  const { orders,failureRate} = req.body;
  const transaction = await sequelize.transaction();
  const io = getIO();
  try {
    const result = [];

    for (let order of orders) {
      const id = randomUUID();

      const newOrder = await Order.create(
        {
          id,
          customerName: order.customerName,
          productName: order.productName,
          quantity: order.quantity,
          Pattern_Type: "Transactional_Log_Tailing"
        },
        { transaction }
      );
      io.emit("order-added", {pattern: "Transactional_Log_Tailing",newOrder});
      result.push(newOrder);
    }
    await transaction.commit();
    res.status(201).json(result);
  } catch (err) {
    await transaction.rollback();
    io.emit("order-add-failed", {pattern: "Transactional_Log_Tailing",order});
    res.status(500).json({ error: err.message });
  }
};


export const getOrders = async (req, res) => {
  try {
    const orders = await Order.findAll({
      order: [["createdAt", "DESC"]],
    });

    res.status(200).json({
      count: orders.length,
      data: orders,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};


export const getOutbox = async (req, res) => {
  try {
    const outbox = await Outbox.findAll({
      order: [["createdAt", "DESC"]],
    });

    res.status(200).json({
      count: outbox.length,
      data: outbox,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};


export const getOutbox_Listen_To_Yourself = async (req, res) => {
  try {
    const outbox = await Outbox_Listen_To_yourself.findAll({
      order: [["createdAt", "DESC"]],
    });

    res.status(200).json({
      count: outbox.length,
      data: outbox,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

export const deleteOrders = async (req, res) => {
  const transaction = await sequelize.transaction();

  try {
    const deletedCount = await Order.destroy({
      where: {},
      truncate: true,
      transaction,
    });

    await transaction.commit();

    res.status(200).json({
      message: "All orders deleted successfully",
      deletedCount,
    });
  } catch (err) {
    await transaction.rollback();

    res.status(500).json({
      error: err.message,
    });
  }
};


export const deleteOutbox = async (req, res) => {
  const transaction = await sequelize.transaction();

  try {
    const deletedCount = await Outbox.destroy({
      where: {},
      truncate: true,
      transaction,
    });

    await transaction.commit();

    res.status(200).json({
      message: "All outbox events deleted successfully",
      deletedCount,
    });
  } catch (err) {
    await transaction.rollback();

    res.status(500).json({
      error: err.message,
    });
  }
};


export const deleteOutbox_Listen_To_Yourself = async (req, res) => {
  const transaction = await sequelize.transaction();

  try {
    const deletedCount = await Outbox_Listen_To_yourself.destroy({
      where: {},
      truncate: true,
      transaction,
    });

    await transaction.commit();

    res.status(200).json({
      message: "All listen-to-yourself outbox events deleted successfully",
      deletedCount,
    });
  } catch (err) {
    await transaction.rollback();

    res.status(500).json({
      error: err.message,
    });
  }
};