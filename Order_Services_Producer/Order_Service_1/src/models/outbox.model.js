import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const Outbox = sequelize.define(
  "Outbox",
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
    tableName: "Outbox_Transactional_Outbox",
    timestamps: true,
  }
);