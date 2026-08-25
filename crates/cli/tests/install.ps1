#Requires -Version 5.1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$Installer = Join-Path $RepositoryRoot "crates/cli/install.ps1"
$TestRoot = Join-Path $env:RUNNER_TEMP "helix-installer-$([Guid]::NewGuid().ToString('N'))"
$Fixture = Join-Path $TestRoot "helix-fixture.exe"
$SourceFile = Join-Path $TestRoot "helix-fixture.cs"
$InstallDir = Join-Path $TestRoot "helix-bin"
$OriginalProcessPath = $env:Path
$OriginalUserPath = [Environment]::GetEnvironmentVariable("Path", "User")

New-Item -ItemType Directory -Path $TestRoot | Out-Null

try {
    $Source = @'
using System;

public static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length == 1 && args[0] == "--version")
        {
            Console.WriteLine("Helix CLI 9.8.7");
            return 0;
        }
        return 1;
    }
}
'@
    Set-Content -LiteralPath $SourceFile -Value $Source -Encoding UTF8
    $Compiler = Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"
    & $Compiler /nologo /target:exe "/out:$Fixture" $SourceFile
    if ($LASTEXITCODE -ne 0) {
        throw "Fixture compilation failed with exit code $LASTEXITCODE."
    }

    & $Installer `
        -InstallDir $InstallDir `
        -Version "v9.8.7" `
        -AssetPath $Fixture `
        -NoPathUpdate `
        -Force

    $Actual = & (Join-Path $InstallDir "helix.exe") --version
    if ($Actual -ne "Helix CLI 9.8.7") {
        throw "Expected 'Helix CLI 9.8.7', got '$Actual'."
    }

    $env:Path = $OriginalProcessPath
    & $Installer `
        -InstallDir $InstallDir `
        -Version "v9.8.7" `
        -AssetPath $Fixture

    $NormalizedInstallDir = $InstallDir.TrimEnd('\')
    $ProcessMatches = @(
        $env:Path -split ';' |
            ForEach-Object { $_.TrimEnd('\') } |
            Where-Object { $_ -ieq $NormalizedInstallDir }
    )
    if ($ProcessMatches.Count -ne 1) {
        throw "Expected '$InstallDir' once in the process PATH, found $($ProcessMatches.Count)."
    }

    $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $UserMatches = @(
        $UserPath -split ';' |
            ForEach-Object { $_.TrimEnd('\') } |
            Where-Object { $_ -ieq $NormalizedInstallDir }
    )
    if ($UserMatches.Count -ne 1) {
        throw "Expected '$InstallDir' once in the user PATH, found $($UserMatches.Count)."
    }
} finally {
    try {
        $env:Path = $OriginalProcessPath
        [Environment]::SetEnvironmentVariable("Path", $OriginalUserPath, "User")
    } finally {
        if (Test-Path -LiteralPath $TestRoot) {
            Remove-Item -LiteralPath $TestRoot -Recurse -Force
        }
    }
}
