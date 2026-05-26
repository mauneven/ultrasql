const assert = require("node:assert/strict");
const test = require("node:test");

const { assetForPlatform } = require("../scripts/platform");

test("maps supported platforms to release assets", () => {
  assert.deepEqual(assetForPlatform("0.0.2", "darwin", "arm64"), {
    archive: "ultrasql-v0.0.2-aarch64-apple-darwin.tar.gz",
    binaryExtension: "",
    target: "aarch64-apple-darwin",
  });
  assert.deepEqual(assetForPlatform("0.0.2", "linux", "x64"), {
    archive: "ultrasql-v0.0.2-x86_64-unknown-linux-gnu.tar.gz",
    binaryExtension: "",
    target: "x86_64-unknown-linux-gnu",
  });
  assert.deepEqual(assetForPlatform("0.0.2", "win32", "x64"), {
    archive: "ultrasql-v0.0.2-x86_64-pc-windows-msvc.zip",
    binaryExtension: ".exe",
    target: "x86_64-pc-windows-msvc",
  });
});

test("rejects unsupported platforms", () => {
  assert.throws(
    () => assetForPlatform("0.0.2", "freebsd", "x64"),
    /unsupported platform/
  );
});
