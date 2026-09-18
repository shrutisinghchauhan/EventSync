import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const Outbox_Listen_To_yourself = sequelize.define(
  "Outbox_Listen_To_yourself",
  {
    id: {
      type: DataTypes.STRING,
      primaryKey: true,
    },
    payload: DataTypes.JSON,
    status: DataTypes.STRING,
    simulationMode: DataTypes.STRING,
    failureRate: DataTypes.FLOAT,
    requestIndex: DataTypes.INTEGER,
    attempts: DataTypes.INTEGER,
    lastError: DataTypes.STRING,
    processedAt: DataTypes.DATE,
  },
  {
    tableName: "Outbox_Listen_To_yourself",
    timestamps: true,
  }
);