import { DataTypes } from "sequelize";
import { sequelize } from "../db/sequelize.js";

export const EventsConsumed = sequelize.define(
  "Events_consumed",
  {
    id: {
      type: DataTypes.STRING,
      defaultValue: DataTypes.UUIDV4,
      primaryKey: true,
    },
    order_id: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    topic: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    partition: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    offset: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    order_event: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
  },
  {
    tableName: "Events_consumed",
    timestamps: true,
  }
);
