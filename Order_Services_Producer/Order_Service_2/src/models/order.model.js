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
    Pattern_Type: {
      type: DataTypes.STRING,
      allowNull: false
    },
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