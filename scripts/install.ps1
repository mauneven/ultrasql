param(
    [string]$Version = $(if ($env:ULTRASQL_VERSION) { $env:ULTRASQL_VERSION } else { "latest" }),
    [string]$Repo = $(if ($env:ULTRASQL_REPO) { $env:ULTRASQL_REPO } else { "mauneven/ultrasql" }),
    [string]$InstallDir = $(if ($env:ULTRASQL_INSTALL_DIR) { $env:ULTRASQL_INSTALL_DIR } else { Join-Path $HOME ".ultrasql\bin" }),
    [switch]$AddToPath
)

$ErrorActionPreference = "Stop"

function Fail-Install {
    param([string]$Message)
    throw "ultrasql install: $Message"
}

function Validate-Repo {
    param([string]$Value)
    if ($Value -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') {
        Fail-Install "invalid repository: $Value"
    }
}

function Validate-ReleaseVersion {
    param([string]$Value)
    if ($Value -notmatch '^v[0-9][A-Za-z0-9._+\-]*$') {
        Fail-Install "invalid release version: $Value"
    }
}

function Validate-ZipMembers {
    param(
        [string]$ZipPath
    )
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $allowed = @(
        "ultrasqld.exe",
        "ultrasql.exe",
        "ultrasql-local.exe",
        "ultrasql.node"
    )
    $zip = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)
    try {
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName.Replace('\', '/')
            if ([string]::IsNullOrWhiteSpace($name) -or
                $name.StartsWith('/') -or
                $name.Contains('../') -or
                $name.Contains('/..') -or
                ($allowed -notcontains $name)) {
                Fail-Install "archive contains unexpected path: $name"
            }
        }
    }
    finally {
        $zip.Dispose()
    }
}

Validate-Repo $Repo
if ($Version -ne "latest") {
    Validate-ReleaseVersion $Version
}

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($arch) {
    "X64" { $target = "x86_64-pc-windows-msvc" }
    default {
        Fail-Install "unsupported Windows architecture: $arch"
    }
}

if ($Version -eq "latest") {
    try {
        $latest = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
        $Version = $latest.tag_name
    }
    catch {
        $tags = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/tags"
        $Version = ($tags | Where-Object { $_.name -match '^v[0-9]' } | Select-Object -First 1).name
    }
}

if ([string]::IsNullOrWhiteSpace($Version)) {
    Fail-Install "could not resolve release version"
}
Validate-ReleaseVersion $Version

$asset = "ultrasql-$Version-$target.zip"
$baseUrl = "https://github.com/$Repo/releases/download/$Version"
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("ultrasql-install-" + [System.Guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $tmp | Out-Null

try {
    $zipPath = Join-Path $tmp $asset
    $sumPath = Join-Path $tmp "$asset.sha256"
    Invoke-WebRequest -Uri "$baseUrl/$asset" -OutFile $zipPath
    Invoke-WebRequest -Uri "$baseUrl/$asset.sha256" -OutFile $sumPath

    $expected = (Get-Content $sumPath -Raw).Trim().Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)[0].ToLowerInvariant()
    if ($expected -notmatch '^[0-9a-f]{64}$') {
        Fail-Install "checksum file does not contain a SHA-256 digest"
    }
    $actual = (Get-FileHash -Algorithm SHA256 $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        Fail-Install "checksum mismatch for $asset"
    }

    $extractDir = Join-Path $tmp "extract"
    Validate-ZipMembers $zipPath
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force
    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item (Join-Path $extractDir "ultrasqld.exe") $InstallDir -Force
    Copy-Item (Join-Path $extractDir "ultrasql.exe") $InstallDir -Force
    Copy-Item (Join-Path $extractDir "ultrasql-local.exe") $InstallDir -Force

    if ($AddToPath) {
        $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
        $paths = $currentPath -split ";" | Where-Object { $_ -ne "" }
        if ($paths -notcontains $InstallDir) {
            [Environment]::SetEnvironmentVariable("Path", ($paths + $InstallDir -join ";"), "User")
            Write-Host "Added $InstallDir to the user PATH. Open a new terminal to use it."
        }
    }

    Write-Host "UltraSQL $Version installed to $InstallDir"
}
finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
