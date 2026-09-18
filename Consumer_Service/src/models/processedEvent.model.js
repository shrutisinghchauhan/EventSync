import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const ProcessedEvent = sequelize.define(
  "Processed_Event",
  {
    order_id: {
      type: DataTypes.STRING,
      primaryKey: true,
    },
  },
  {
    tableName: "Processed_Event",
    timestamps: true,
  }
);
