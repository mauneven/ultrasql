import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "./generated/prisma/client.js";

const dsn = process.argv[2];
if (!dsn) {
  console.error("usage: prisma_cert.mjs DATABASE_URL");
  process.exit(2);
}

function assertRows(actual, expected, context) {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${context}: expected ${expectedJson}, got ${actualJson}`);
  }
}

const adapter = new PrismaPg({ connectionString: dsn });
const prisma = new PrismaClient({ adapter });

try {
  await prisma.$connect();

  let rows = await prisma.$queryRaw`SELECT id, name FROM users WHERE id = ${3}`;
  assertRows(rows, [{ id: 3, name: "Linus" }], "Prisma parameterized SELECT");

  await prisma.$executeRawUnsafe(
    "CREATE TABLE prisma_cert (id INT NOT NULL PRIMARY KEY, label TEXT NOT NULL)"
  );
  await prisma.prismaCert.create({ data: { id: 1, label: "alpha" } });
  await prisma.prismaCert.create({ data: { id: 2, label: "beta" } });
  rows = await prisma.prismaCert.findMany({
    orderBy: { id: "asc" },
    select: { id: true, label: true }
  });
  assertRows(
    rows,
    [
      { id: 1, label: "alpha" },
      { id: 2, label: "beta" }
    ],
    "Prisma Client create/query"
  );

  try {
    await prisma.$transaction(async (tx) => {
      await tx.prismaCert.create({ data: { id: 3, label: "rollback" } });
      throw new Error("rollback Prisma transaction");
    });
  } catch (err) {
    if (err.message !== "rollback Prisma transaction") {
      throw err;
    }
  }
  const count = await prisma.prismaCert.count();
  if (count !== 2) {
    throw new Error(`Prisma transaction rollback: expected 2, got ${count}`);
  }

  try {
    await prisma.$transaction(async (tx) => {
      await tx.$queryRawUnsafe("SELECT missing_column FROM prisma_cert");
    });
    throw new Error("Prisma expected missing-column failure");
  } catch (err) {
    if (err.message === "Prisma expected missing-column failure") {
      throw err;
    }
  }
  rows = await prisma.prismaCert.findMany({
    orderBy: { id: "asc" },
    select: { id: true }
  });
  assertRows(rows, [{ id: 1 }, { id: 2 }], "Prisma recovery after error");
} finally {
  await prisma.$disconnect();
}
