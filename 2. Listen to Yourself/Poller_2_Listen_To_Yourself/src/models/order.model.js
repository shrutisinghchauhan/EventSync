import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const Order = sequelize.define(
  "Order",
  {
    id: {
      type: DataTypes.STRING,
      primaryKey: true,
    },
    customerName: DataTypes.STRING,
    productName: DataTypes.STRING,
    quantity: DataTypes.INTEGER,
    status: {
      type: DataTypes.STRING,
      defaultValue: "CREATED",
    },
  },
  {
    tableName: "Order",
    timestamps: true,
  }
);