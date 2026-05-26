"use strict";

const fs = require("node:fs");
const path = require("node:path");

const { assetForPlatform } = require("./platform");
const { install, vendorDirFor } = require("./install");

let cachedNative = null;

function packageRoot() {
  return path.resolve(__dirname, "..");
}

function packageVersion() {
  return require(path.join(packageRoot(), "package.json")).version;
}

function nativeFileName() {
  return "ultrasql.node";
}

function nativePlan() {
  return assetForPlatform(process.env.ULTRASQL_VERSION || packageVersion());
}

function nativePathFor(plan) {
  return path.join(vendorDirFor(plan), nativeFileName());
}

function nativeCandidates() {
  const override = process.env.ULTRASQL_NATIVE_PATH;
  if (override) {
    return [path.resolve(override)];
  }
  const plan = nativePlan();
  return [
    nativePathFor(plan),
    path.join(packageRoot(), "prebuilds", plan.target, nativeFileName()),
  ];
}

function loadNative() {
  if (cachedNative) {
    return cachedNative;
  }
  const tried = [];
  for (const candidate of nativeCandidates()) {
    tried.push(candidate);
    if (fs.existsSync(candidate)) {
      cachedNative = require(candidate);
      return cachedNative;
    }
  }
  throw new Error(
    [
      "UltraSQL embedded native addon is not installed.",
      "Use `await Database.open(\":memory:\")` so the package can download the release archive,",
      "or set ULTRASQL_NATIVE_PATH to a built ultrasql.node file.",
      `Tried: ${tried.join(", ")}`,
    ].join(" ")
  );
}

async function ensureNativeInstalled() {
  if (process.env.ULTRASQL_NATIVE_PATH) {
    loadNative();
    return;
  }
  const plan = nativePlan();
  const nativePath = nativePathFor(plan);
  if (!fs.existsSync(nativePath)) {
    await install({ requireNative: true });
  }
  if (!fs.existsSync(nativePath)) {
    throw new Error(`${nativeFileName()} missing from UltraSQL release archive ${plan.archive}`);
  }
  loadNative();
}

module.exports = {
  ensureNativeInstalled,
  loadNative,
  nativeFileName,
  nativePathFor,
};
