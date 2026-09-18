import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const ProcessedEvent = sequelize.define(
  "ProcessedEvent",
  {
    eventId: {
      type: DataTypes.STRING,
      primaryKey: true,
    },
  },
  {
    tableName: "ProcessedEvent",
    timestamps: true,
  }
);