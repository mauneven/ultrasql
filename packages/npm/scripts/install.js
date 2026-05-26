"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const { assetForPlatform, normalizeTag } = require("./platform");

const PACKAGE_ROOT = path.resolve(__dirname, "..");
const BINARIES = ["ultrasqld", "ultrasql", "ultrasql-local"];
const NATIVE_MODULE = "ultrasql.node";

function packageVersion() {
  return require(path.join(PACKAGE_ROOT, "package.json")).version;
}

function downloadPlan({
  packageVersion,
  versionOverride,
  repo = process.env.ULTRASQL_REPO || "mauneven/ultrasql",
  platform = process.platform,
  arch = process.arch,
}) {
  const tag = normalizeTag(versionOverride || packageVersion);
  const asset = assetForPlatform(tag, platform, arch);
  const assetUrl = `https://github.com/${repo}/releases/download/${tag}/${asset.archive}`;
  return {
    ...asset,
    assetUrl,
    checksumUrl: `${assetUrl}.sha256`,
    repo,
    tag,
  };
}

function fetchBuffer(url, redirects = 0) {
  if (redirects > 5) {
    return Promise.reject(new Error(`too many redirects for ${url}`));
  }
  return new Promise((resolve, reject) => {
    https
      .get(url, (response) => {
        if (
          response.statusCode >= 300 &&
          response.statusCode < 400 &&
          response.headers.location
        ) {
          response.resume();
          resolve(fetchBuffer(new URL(response.headers.location, url).toString(), redirects + 1));
          return;
        }
        if (response.statusCode !== 200) {
          response.resume();
          reject(new Error(`download failed: ${url} returned ${response.statusCode}`));
          return;
        }
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => resolve(Buffer.concat(chunks)));
      })
      .on("error", reject);
  });
}

async function downloadFile(url, destination) {
  const data = await fetchBuffer(url);
  fs.writeFileSync(destination, data);
  return data;
}

function verifySha256(archivePath, checksumText) {
  const expected = checksumText.trim().split(/\s+/)[0];
  if (!/^[a-fA-F0-9]{64}$/.test(expected)) {
    throw new Error("release checksum file does not contain a SHA-256 digest");
  }
  const actual = crypto
    .createHash("sha256")
    .update(fs.readFileSync(archivePath))
    .digest("hex");
  if (actual !== expected.toLowerCase()) {
    throw new Error(`checksum mismatch for ${path.basename(archivePath)}`);
  }
}

function extractArchive(plan, archivePath, extractDir) {
  const command =
    plan.archive.endsWith(".zip")
      ? {
          bin: "powershell.exe",
          args: [
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "& { param($archive, $dest) Expand-Archive -LiteralPath $archive -DestinationPath $dest -Force }",
            archivePath,
            extractDir,
          ],
        }
      : { bin: "tar", args: ["-xzf", archivePath, "-C", extractDir] };
  const result = spawnSync(command.bin, command.args, { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error(`failed to extract ${plan.archive}`);
  }
}

function findBinary(root, fileName) {
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const fullPath = path.join(root, entry.name);
    if (entry.isFile() && entry.name === fileName) {
      return fullPath;
    }
    if (entry.isDirectory()) {
      const found = findBinary(fullPath, fileName);
      if (found) {
        return found;
      }
    }
  }
  return null;
}

function copyBinaries(plan, extractDir, vendorDir) {
  fs.mkdirSync(vendorDir, { recursive: true });
  for (const binary of BINARIES) {
    const fileName = `${binary}${plan.binaryExtension}`;
    const source = findBinary(extractDir, fileName);
    if (!source) {
      throw new Error(`${fileName} missing from ${plan.archive}`);
    }
    const destination = path.join(vendorDir, fileName);
    fs.copyFileSync(source, destination);
    if (plan.binaryExtension === "") {
      fs.chmodSync(destination, 0o755);
    }
  }
  const nativeSource = findBinary(extractDir, NATIVE_MODULE);
  if (nativeSource) {
    fs.copyFileSync(nativeSource, path.join(vendorDir, NATIVE_MODULE));
  }
  fs.writeFileSync(path.join(vendorDir, ".release-tag"), `${plan.tag}\n`);
}

function vendorDirFor(plan) {
  return path.join(PACKAGE_ROOT, "vendor", plan.target);
}

function isInstalled(plan, options = {}) {
  const vendorDir = vendorDirFor(plan);
  const marker = path.join(vendorDir, ".release-tag");
  if (!fs.existsSync(marker) || fs.readFileSync(marker, "utf8").trim() !== plan.tag) {
    return false;
  }
  const hasBinaries = BINARIES.every((binary) =>
    fs.existsSync(path.join(vendorDir, `${binary}${plan.binaryExtension}`))
  );
  if (!hasBinaries) {
    return false;
  }
  if (options.requireNative && !fs.existsSync(path.join(vendorDir, NATIVE_MODULE))) {
    return false;
  }
  return true;
}

async function install(options = {}) {
  if (process.env.ULTRASQL_NPM_SKIP_DOWNLOAD === "1") {
    return;
  }
  const plan = downloadPlan({
    packageVersion: packageVersion(),
    versionOverride: process.env.ULTRASQL_VERSION,
  });
  if (isInstalled(plan, options)) {
    return;
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ultrasql-npm-"));
  try {
    const archivePath = path.join(tmpDir, plan.archive);
    const checksumText = (await fetchBuffer(plan.checksumUrl)).toString("utf8");
    await downloadFile(plan.assetUrl, archivePath);
    verifySha256(archivePath, checksumText);
    const extractDir = path.join(tmpDir, "extract");
    fs.mkdirSync(extractDir);
    extractArchive(plan, archivePath, extractDir);
    copyBinaries(plan, extractDir, vendorDirFor(plan));
  } finally {
    fs.rmSync(tmpDir, { force: true, recursive: true });
  }
}

if (require.main === module) {
  install().catch((error) => {
    console.error(`ultrasql npm install failed: ${error.message}`);
    process.exitCode = 1;
  });
}

module.exports = {
  downloadPlan,
  install,
  isInstalled,
  vendorDirFor,
};
