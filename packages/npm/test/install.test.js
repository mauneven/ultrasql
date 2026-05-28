const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const { downloadPlan, isInstalled, vendorDirFor } = require("../scripts/install");

test("builds checksum-verified release download plan", () => {
  const plan = downloadPlan({
    packageVersion: "0.0.7",
    platform: "linux",
    arch: "arm64",
    repo: "mauneven/ultrasql",
  });

  assert.equal(plan.tag, "v0.0.7");
  assert.equal(
    plan.assetUrl,
    "https://github.com/mauneven/ultrasql/releases/download/v0.0.7/ultrasql-v0.0.7-aarch64-unknown-linux-gnu.tar.gz"
  );
  assert.equal(`${plan.assetUrl}.sha256`, plan.checksumUrl);
});

test("accepts explicit v-prefixed release version override", () => {
  const plan = downloadPlan({
    packageVersion: "0.0.7",
    versionOverride: "v0.0.1",
    platform: "darwin",
    arch: "x64",
    repo: "mauneven/ultrasql",
  });

  assert.equal(plan.tag, "v0.0.1");
  assert.equal(
    plan.assetUrl,
    "https://github.com/mauneven/ultrasql/releases/download/v0.0.1/ultrasql-v0.0.1-x86_64-apple-darwin.tar.gz"
  );
});

test("native install check requires ultrasql.node when requested", () => {
  const plan = downloadPlan({
    packageVersion: "0.0.7",
    platform: process.platform,
    arch: process.arch,
    repo: "mauneven/ultrasql",
  });
  const vendorDir = vendorDirFor(plan);
  const backupDir = fs.existsSync(vendorDir)
    ? fs.mkdtempSync(path.join(os.tmpdir(), "ultrasql-vendor-backup-"))
    : null;
  const backupPath = backupDir ? path.join(backupDir, path.basename(vendorDir)) : null;
  if (backupPath) {
    fs.renameSync(vendorDir, backupPath);
  }
  try {
    fs.mkdirSync(vendorDir, { recursive: true });
    fs.writeFileSync(path.join(vendorDir, ".release-tag"), `${plan.tag}\n`);
    for (const binary of ["ultrasqld", "ultrasql", "ultrasql-local"]) {
      fs.writeFileSync(path.join(vendorDir, `${binary}${plan.binaryExtension}`), "");
    }
    assert.equal(isInstalled(plan), true);
    assert.equal(isInstalled(plan, { requireNative: true }), false);
    fs.writeFileSync(path.join(vendorDir, "ultrasql.node"), "");
    assert.equal(isInstalled(plan, { requireNative: true }), true);
  } finally {
    fs.rmSync(vendorDir, { recursive: true, force: true });
    if (backupPath) {
      fs.renameSync(backupPath, vendorDir);
      fs.rmSync(backupDir, { recursive: true, force: true });
    }
  }
});
