import { sequelize } from "./sequelize.js";
import pkg from "pg";

const { Pool } = pkg;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const connectDB = async () => {
  try {
    await sequelize.authenticate();

    console.log(
      `PostgreSQL Connected! Host: ${process.env.DB_HOST}, Port: ${process.env.DB_PORT}`
    );

    const client = await pool.connect();
    console.log(" PG Pool Connected!");
    client.release();

  } catch (e) {
    console.error(" POSTGRESQL CONNECTION FAILED:", e.message);
    process.exit(1);
  }
};

export default connectDB;