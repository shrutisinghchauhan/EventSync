import { sequelize } from "./sequelize.js";
import "../models/eventsConsumed.model.js";
import "../models/processedEvent.model.js";

const connectDB = async () => {
  try {
    await sequelize.authenticate();
    await sequelize.sync();

    console.log(
      `Consumer PostgreSQL Connected! Host: ${process.env.DB_HOST}, Port: ${process.env.DB_PORT}`
    );
  } catch (e) {
    console.error("CONSUMER POSTGRESQL CONNECTION FAILED:", e.message);
    process.exit(1);
  }
};

export default connectDB;
