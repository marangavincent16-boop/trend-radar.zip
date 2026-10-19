import { neon } from "@neondatabase/serverless";

// Created on demand so the build works without a database connection.
export const db = () => neon(process.env.DATABASE_URL!);
