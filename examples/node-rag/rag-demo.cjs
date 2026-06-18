#!/usr/bin/env node
// UltraSQL — embedded RAG / agent-memory demo (Node.js, zero npm dependencies).
//
// The pitch: one embeddable, ACID, Postgres-compatible engine where text +
// vectors + JSON metadata live in ONE table and are ranked together in ONE
// transaction — then survive a process restart. No Pinecone + Elasticsearch +
// Redis to stitch together and keep in sync.
//
//   ./build.sh && node rag-demo.cjs
//
// `build.sh` compiles the native addon (crates/ultrasql-node) and drops
// `ultrasql_node.node` next to this file.

const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");

let Database;
try {
  ({ Database } = require("./ultrasql_node.node"));
} catch (err) {
  console.error("Native addon not found — run ./build.sh first (it compiles crates/ultrasql-node).");
  process.exit(1);
}

const DATA_DIR = path.join(os.tmpdir(), "ultrasql-rag-demo");

// The agent's query — text + the embedding YOUR model produced for it. The
// embedding is supplied by the caller; UltraSQL stores and ranks, it does not
// embed (no bundled model, no hidden network).
const QUERY_TEXT = "failed invoice payment";
const QUERY_VEC = "VECTOR '[1,0,0]'";

// One statement: filter by tenant metadata, then rank by RRF fusion of dense
// vector similarity and BM25 lexical relevance over the same MVCC table.
const RETRIEVE =
  "SELECT id, body FROM memories " +
  "WHERE metadata @> '{\"tenant\":\"acme\"}' " +
  `ORDER BY hybrid_search(body, '${QUERY_TEXT}', embedding, ${QUERY_VEC}, 'rrf') DESC ` +
  "LIMIT 3";

function retrieve(db, heading) {
  console.log(heading);
  for (const [id, body] of db.execute(RETRIEVE).rows) {
    console.log(`   #${id}  ${body}`);
  }
}

// --- "after restart" phase: a *separate* OS process reopens the same dir ---
if (process.argv[2] === "--reopen") {
  const db = new Database(DATA_DIR); // WAL replay happens on open
  retrieve(db, "After restart (fresh process, WAL-recovered) — same answer:");
  const n = db.execute("SELECT COUNT(*) FROM memories").rows[0][0];
  console.log(`\n   ${n} memories survived the restart. ACID + durable, one binary.`);
  process.exit(0);
}

// --- ingest phase ---
fs.rmSync(DATA_DIR, { recursive: true, force: true });
const db = new Database(DATA_DIR); // WAL-backed, on-disk

db.execute(
  "CREATE TABLE memories (id INT NOT NULL, body TEXT, embedding VECTOR(3), metadata JSONB)"
);
db.execute("CREATE INDEX memories_hnsw ON memories USING hnsw (embedding vector_l2_ops)");

// Text + embedding + metadata, committed atomically in one transaction.
db.execute("BEGIN");
db.execute(
  "INSERT INTO memories VALUES " +
    // doc 2 is the best acme match (strong text + near vector) and outranks the
    // lower-id doc 1, so the fused ranking is visibly not just id order.
    "(1, 'payment retry succeeded',             '[0.5,0.3,0]', '{\"tenant\":\"acme\",\"kind\":\"billing\"}'), " +
    "(2, 'invoice payment failed for customer', '[0.9,0.1,0]', '{\"tenant\":\"acme\",\"kind\":\"billing\"}'), " +
    "(3, 'user updated profile photo',          '[0,0,1]',     '{\"tenant\":\"acme\",\"kind\":\"profile\"}'), " +
    "(4, 'invoice payment failed urgent',       '[0.95,0,0]',  '{\"tenant\":\"other\",\"kind\":\"billing\"}')"
);
db.execute("COMMIT");

const ingested = db.execute("SELECT COUNT(*) FROM memories").rows[0][0];
console.log(`Ingested ${ingested} memories (text + vector + JSON) in one transaction.\n`);

// A standalone vector store ranks by embedding distance alone — and the nearest
// embedding here belongs to another tenant's memory.
const nearest = db.execute(
  "SELECT id, body, metadata->>'tenant' AS tenant FROM memories " +
    `ORDER BY embedding <-> ${QUERY_VEC} LIMIT 1`
).rows[0];
console.log("Vector similarity ALONE (what a bolt-on vector store returns):");
console.log(`   #${nearest[0]}  ${nearest[1]}   (tenant=${nearest[2]} — wrong tenant!)\n`);

retrieve(db, "UltraSQL — one query fusing vector + BM25 + tenant metadata:");
console.log("   (doc 4's embedding is nearest, but the tenant filter excludes it)\n");

// --- restart: hand off to a brand-new process that reopens the same dir ---
execFileSync(process.execPath, [__filename, "--reopen"], { stdio: "inherit" });
