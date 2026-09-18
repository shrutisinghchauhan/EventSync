import { sequelize } from "./sequelize.js";

const connectDB = async () => {
  try {
    await sequelize.authenticate();
    console.log(
      ` PostgreSQL Connected! Host: ${process.env.DB_HOST}, Port: ${process.env.DB_PORT}`
    );
  } catch (e) {
    console.error("POSTGRESQL CONNECTION FAILED:", e.message);
    process.exit(1);
  }
};

export default connectDB;