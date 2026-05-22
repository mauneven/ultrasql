param(
    [string]$Version = $(if ($env:ULTRASQL_VERSION) { $env:ULTRASQL_VERSION } else { "latest" }),
    [string]$Repo = $(if ($env:ULTRASQL_REPO) { $env:ULTRASQL_REPO } else { "mauneven/ultrasql" }),
    [string]$InstallDir = $(if ($env:ULTRASQL_INSTALL_DIR) { $env:ULTRASQL_INSTALL_DIR } else { Join-Path $HOME ".ultrasql\bin" }),
    [switch]$AddToPath
)

$ErrorActionPreference = "Stop"

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($arch) {
    "X64" { $target = "x86_64-pc-windows-msvc" }
    default {
        throw "unsupported Windows architecture: $arch"
    }
}

if ($Version -eq "latest") {
    $latest = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
    $Version = $latest.tag_name
}

if ([string]::IsNullOrWhiteSpace($Version)) {
    throw "could not resolve release version"
}

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
    $actual = (Get-FileHash -Algorithm SHA256 $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        throw "checksum mismatch for $asset"
    }

    $extractDir = Join-Path $tmp "extract"
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
