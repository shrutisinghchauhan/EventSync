import { defineConfig } from '@prisma/config';

const DB_URL = "postgresql://myuser:mypassword@localhost:5433/consumer_db?schema=public";

export default defineConfig({
  datasource: {
    url: DB_URL
  },
});
