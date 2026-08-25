#Requires -Version 5.1

<#
.SYNOPSIS
Installs the latest Helix CLI release on Windows.

.PARAMETER InstallDir
Target directory. Defaults to %LOCALAPPDATA%\Helix\bin.

.PARAMETER Version
Release tag to install. Defaults to the latest GitHub release.

.PARAMETER Force
Reinstall even when the requested version is already installed.

.PARAMETER NoPathUpdate
Do not persist InstallDir in the user PATH. Intended for CI and managed hosts.

.PARAMETER AssetPath
Use a local release asset instead of downloading one. Intended for CI tests.
#>

[CmdletBinding()]
param(
    [string]$InstallDir = "",
    [string]$Version = "",
    [switch]$Force,
    [switch]$NoPathUpdate,
    [string]$AssetPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Repository = "HelixDB/helix-db"
$AssetName = "helix-x86_64-pc-windows-msvc.exe"

function Get-NormalizedVersion {
    param([Parameter(Mandatory = $true)][string]$Value)

    if ($Value -notmatch '^v?(\d+\.\d+\.\d+)$') {
        throw "Invalid release version '$Value'. Expected vMAJOR.MINOR.PATCH."
    }
    return $Matches[1]
}

function Get-InstalledVersion {
    param([Parameter(Mandatory = $true)][string]$BinaryPath)

    if (-not (Test-Path -LiteralPath $BinaryPath -PathType Leaf)) {
        return $null
    }

    $Output = & $BinaryPath --version 2>$null
    if ($LASTEXITCODE -ne 0 -or $Output -notmatch '(\d+\.\d+\.\d+)') {
        return $null
    }
    return $Matches[1]
}

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "The PowerShell installer supports Windows only."
}

$Architecture = if ($env:PROCESSOR_ARCHITEW6432) {
    $env:PROCESSOR_ARCHITEW6432
} else {
    $env:PROCESSOR_ARCHITECTURE
}
if ($Architecture -notin @("AMD64", "x86_64")) {
    throw "Unsupported Windows architecture '$Architecture'. The current release asset is x86-64."
}

if ([string]::IsNullOrWhiteSpace($InstallDir)) {
    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        throw "LOCALAPPDATA is not set. Pass -InstallDir explicitly."
    }
    $InstallDir = Join-Path $env:LOCALAPPDATA "Helix\bin"
}
$InstallDir = [IO.Path]::GetFullPath($InstallDir)
$BinaryPath = Join-Path $InstallDir "helix.exe"

if ([string]::IsNullOrWhiteSpace($Version)) {
    Write-Host "Fetching the latest Helix CLI release..."
    $Release = Invoke-RestMethod `
        -Headers @{ "User-Agent" = "Helix-CLI-Installer" } `
        -Uri "https://api.github.com/repos/$Repository/releases/latest"
    $Version = [string]$Release.tag_name
}

$NormalizedVersion = Get-NormalizedVersion $Version
$ReleaseTag = "v$NormalizedVersion"
$InstalledVersion = Get-InstalledVersion $BinaryPath
$InstallRequired = $Force -or $InstalledVersion -ne $NormalizedVersion

if (-not $InstallRequired) {
    Write-Host "Helix CLI $NormalizedVersion is already installed at $BinaryPath."
    $VerifiedVersion = $InstalledVersion
} else {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    $StagedPath = Join-Path $InstallDir ".helix-$([Guid]::NewGuid().ToString('N')).exe"

    try {
        if ([string]::IsNullOrWhiteSpace($AssetPath)) {
            $DownloadUrl = "https://github.com/$Repository/releases/download/$ReleaseTag/$AssetName"
            Write-Host "Downloading $DownloadUrl"
            Invoke-WebRequest `
                -Headers @{ "User-Agent" = "Helix-CLI-Installer" } `
                -Uri $DownloadUrl `
                -OutFile $StagedPath `
                -UseBasicParsing
        } else {
            $ResolvedAsset = (Resolve-Path -LiteralPath $AssetPath).Path
            Copy-Item -LiteralPath $ResolvedAsset -Destination $StagedPath
        }

        $StagedVersion = Get-InstalledVersion $StagedPath
        if ($StagedVersion -ne $NormalizedVersion) {
            throw "Downloaded asset verification failed. Expected $NormalizedVersion, got '$StagedVersion'."
        }

        Move-Item -LiteralPath $StagedPath -Destination $BinaryPath -Force
    } finally {
        if (Test-Path -LiteralPath $StagedPath) {
            Remove-Item -LiteralPath $StagedPath -Force
        }
    }

    $VerifiedVersion = Get-InstalledVersion $BinaryPath
    if ($VerifiedVersion -ne $NormalizedVersion) {
        throw "Installation verification failed. Expected $NormalizedVersion, got '$VerifiedVersion'."
    }
}

$ProcessPathExists = $false
foreach ($Entry in ($env:Path -split ';')) {
    if ($Entry.TrimEnd('\') -ieq $InstallDir.TrimEnd('\')) {
        $ProcessPathExists = $true
        break
    }
}

if (-not $ProcessPathExists) {
    $env:Path = "$InstallDir;$env:Path"
}

if (-not $NoPathUpdate) {
    $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $PathExists = $false
    foreach ($Entry in ($UserPath -split ';')) {
        if ($Entry.TrimEnd('\') -ieq $InstallDir.TrimEnd('\')) {
            $PathExists = $true
            break
        }
    }

    if (-not $PathExists) {
        $UpdatedPath = if ([string]::IsNullOrWhiteSpace($UserPath)) {
            $InstallDir
        } else {
            "$InstallDir;$UserPath"
        }
        [Environment]::SetEnvironmentVariable("Path", $UpdatedPath, "User")
        Write-Host "Added $InstallDir to your user PATH. Restart PowerShell to use it in a new session."
    }
}

if ($InstallRequired) {
    Write-Host "Installed Helix CLI $VerifiedVersion to $BinaryPath."
}
