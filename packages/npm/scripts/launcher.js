"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const { assetForPlatform } = require("./platform");
const { install, vendorDirFor } = require("./install");

async function run(binaryName) {
  const packageRoot = path.resolve(__dirname, "..");
  const version = process.env.ULTRASQL_VERSION || require(path.join(packageRoot, "package.json")).version;
  const plan = assetForPlatform(version);
  const binaryPath = path.join(vendorDirFor(plan), `${binaryName}${plan.binaryExtension}`);

  if (!fs.existsSync(binaryPath)) {
    await install();
  }
  if (!fs.existsSync(binaryPath)) {
    console.error(`ultrasql npm launcher could not find ${binaryName}`);
    process.exit(1);
  }

  const result = spawnSync(binaryPath, process.argv.slice(2), { stdio: "inherit" });
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  if (result.signal) {
    console.error(`${binaryName} terminated by signal ${result.signal}`);
    process.exit(1);
  }
  process.exit(result.status ?? 0);
}

module.exports = { run };
