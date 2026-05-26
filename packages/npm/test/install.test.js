const assert = require("node:assert/strict");
const test = require("node:test");

const { downloadPlan } = require("../scripts/install");

test("builds checksum-verified release download plan", () => {
  const plan = downloadPlan({
    packageVersion: "0.0.4",
    platform: "linux",
    arch: "arm64",
    repo: "mauneven/ultrasql",
  });

  assert.equal(plan.tag, "v0.0.4");
  assert.equal(
    plan.assetUrl,
    "https://github.com/mauneven/ultrasql/releases/download/v0.0.4/ultrasql-v0.0.4-aarch64-unknown-linux-gnu.tar.gz"
  );
  assert.equal(`${plan.assetUrl}.sha256`, plan.checksumUrl);
});

test("accepts explicit v-prefixed release version override", () => {
  const plan = downloadPlan({
    packageVersion: "0.0.4",
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
