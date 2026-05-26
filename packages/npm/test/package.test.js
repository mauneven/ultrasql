const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const packageJson = require("../package.json");

test("does not require install-time build approval", () => {
  assert.equal(packageJson.scripts.postinstall, undefined);
});

test("publishes discoverable npm metadata", () => {
  assert.match(packageJson.description, /Embedded UltraSQL database API/);
  assert.equal(packageJson.main, "index.js");
  assert.equal(packageJson.types, "index.d.ts");
  for (const keyword of [
    "database",
    "sql",
    "postgresql",
    "pgwire",
    "embedded",
    "node-api",
    "bun",
    "vector",
    "rust",
  ]) {
    assert.ok(packageJson.keywords.includes(keyword), `missing keyword ${keyword}`);
  }
  assert.deepEqual(packageJson.os, ["darwin", "linux", "win32"]);
  assert.deepEqual(packageJson.cpu, ["x64", "arm64"]);
});

test("readme documents node usage and binary behavior", () => {
  const readme = fs.readFileSync(path.join(__dirname, "..", "README.md"), "utf8");
  for (const needle of [
    "Database.open(\":memory:\")",
    "db.prepare",
    "pnpm add pg",
    "import pg from \"pg\"",
    "Supported Targets",
    "No install-time `postinstall` script",
    "UltraSQL is pre-alpha",
  ]) {
    assert.ok(readme.includes(needle), `README missing ${needle}`);
  }
});
