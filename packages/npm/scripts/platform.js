"use strict";

const TARGETS = {
  "darwin:arm64": "aarch64-apple-darwin",
  "darwin:x64": "x86_64-apple-darwin",
  "linux:arm64": "aarch64-unknown-linux-gnu",
  "linux:x64": "x86_64-unknown-linux-gnu",
  "win32:x64": "x86_64-pc-windows-msvc",
};

function normalizeTag(version) {
  const trimmed = String(version || "").trim();
  if (!trimmed) {
    throw new Error("release version is empty");
  }
  return trimmed.startsWith("v") ? trimmed : `v${trimmed}`;
}

function assetForPlatform(version, platform = process.platform, arch = process.arch) {
  const target = TARGETS[`${platform}:${arch}`];
  if (!target) {
    throw new Error(`unsupported platform: ${platform}/${arch}`);
  }

  const tag = normalizeTag(version);
  const archiveExtension = platform === "win32" ? "zip" : "tar.gz";
  const binaryExtension = platform === "win32" ? ".exe" : "";
  return {
    archive: `ultrasql-${tag}-${target}.${archiveExtension}`,
    binaryExtension,
    target,
  };
}

module.exports = {
  assetForPlatform,
  normalizeTag,
};
