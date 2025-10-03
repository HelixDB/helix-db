# install.ps1 - Helix CLI Installer
# Windows PowerShell script for installing Helix CLI

param(
    [switch]$Verbose,
    [string]$Tag = "latest",
    [string]$Repo = "HelixDB/helix-db",
    [switch]$Uninstall,
    [switch]$Help,
    [string]$Dir,
    [switch]$System,
    [switch]$Force
)


# Version and configuration
$SCRIPT_VERSION = "1.0.0"
$BINARY_NAME = "helix"
$GITHUB_API_URL = "https://api.github.com/repos/$Repo/releases"
$ErrorActionPreference = 'Stop'

# Http client
$http = [System.Net.Http.HttpClient]::new()
$http.Timeout = [timespan]::FromMinutes(5)
$http.DefaultRequestHeaders.UserAgent.ParseAdd("helix-installer/$SCRIPT_VERSION")

# Color support detection
$SupportsColor = $false
if ($Host.UI.RawUI -and $Host.UI.RawUI.ForegroundColor) {
    $SupportsColor = $true
}

# Helper functions
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$ForegroundColor = "White",
        [switch]$NoNewline
    )
    
    if ($SupportsColor) {
        $params = @{
            Object          = $Message
            ForegroundColor = $ForegroundColor
        }
        if ($NoNewline) { $params.NoNewline = $true }
        Write-Host @params
    }
    else {
        if ($NoNewline) {
            Write-Host $Message -NoNewline
        }
        else {
            Write-Host $Message
        }
    }
}

function Write-Info { param([string]$Message) Write-ColorOutput $Message "Cyan" }
function Write-Success { param([string]$Message) Write-ColorOutput $Message "Green" }
function Write-Warning { param([string]$Message) Write-ColorOutput $Message "Yellow" }
function Write-Error { param([string]$Message) Write-ColorOutput $Message "Red" }

function Show-Help {
    @"
helix-cli Installer v$SCRIPT_VERSION

USAGE:
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex [OPTIONS]

OPTIONS:
    -Verbose            Show detailed output
    -Dir <DIR>          Install directory (default: $env:USERPROFILE\.local\bin)
    -Tag <TAG>          Install specific version tag (default: latest)
    -System             System install (requires admin)
    -Force              Force install even if same version exists
    -Uninstall          Remove helix-cli from system
    -Help               Show this help message

EXAMPLES:
    # Basic installation
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex

    # Custom installation path
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex -Dir "C:\Tools\helix"

    # System install (requires admin)
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex -System

    # Force reinstall
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex -Force

    # Uninstall
    iwr -useb https://raw.githubusercontent.com/$Repo/main/install.ps1 | iex -Uninstall
"@
    exit 0
}

function Get-Architecture {
    if ([Environment]::Is64BitOperatingSystem) {
        return "amd64"
    }
    if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") {
        Write-Error "ARM64 architecture detected but is not supported in current releases."
        Write-ColorOutput "Currently available architectures: x86_64 (amd64)" "Yellow"
        Write-ColorOutput "If you need ARM64 support, please open a feature request:" "White"
        Write-ColorOutput "👉 https://github.com/$Repo/issues" "Cyan"

        exit 1
    }
    return "amd64"
}

function Get-LatestRelease {
    param([string]$VersionTag)
    
    $headers = @{
        "Accept"     = "application/vnd.github.v3+json"
        "User-Agent" = "helix-installer/$SCRIPT_VERSION"
    }
    
    try {
        $url = if ($VersionTag -eq "latest") { "$GITHUB_API_URL/latest" } else { "$GITHUB_API_URL/tags/$VersionTag" }
        Write-Verbose "Fetching release info from: $url"
        
        $response = Invoke-WebRequest -Uri $url -Headers $headers -UseBasicParsing
        return $response.Content | ConvertFrom-Json
    }
    catch {
        throw "Failed to fetch release information: $($_.Exception.Message)"
    }
}

function Get-AssetInfo {
    param(
        [object]$Release,
        [string]$Architecture
    )

    # Map amd64 to x86_64 for Rust target format
    $rustArch = "x86_64"
    
    # new naming format starting with v2.0.2
    $assetName = "helix-$rustArch-pc-windows-msvc.exe"
    $asset = $Release.assets | Where-Object { $_.name -eq $assetName }
    
    # Fallback to old naming format (pre-v2.0.2)
    if (-not $asset) {
        $oldAssetName = "helix-cli-windows-amd64.exe"
        $asset = $Release.assets | Where-Object { $_.name -eq $oldAssetName }
        
        if (-not $asset) {
            throw "Could not find asset for Windows amd64 in release $($Release.tag_name). Expected: $assetName or $oldAssetName"
        }
    }
    
    # Find corresponding checksum file
    $checksumAsset = $Release.assets | Where-Object { $_.name -eq "$($asset.name).sha256" }
    
    return @{
        DownloadUrl = $asset.browser_download_url
        FileName    = $asset.name
        Size        = $asset.size
        ChecksumUrl = if ($checksumAsset) { $checksumAsset.browser_download_url } else { $null }
    }
}

function Start-FileDownload {
    param(
        [string]$Url,
        [string]$OutputPath,
        [string]$ProgressMessage = "Download Progress",
        [string]$DoneMessage = "Download complete"
    )

    try {
        $resp = $http.GetAsync($Url, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
        $resp.EnsureSuccessStatusCode()
        
        $contentLen = $resp.Content.Headers.ContentLength
        $inStream = $resp.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
        $outStream = [System.IO.File]::OpenWrite($OutputPath)

        $buffer = [byte[]]::new(81920)
        $total = 0
        $spinner = @('|', '/', '-', '\')
        $i = 0

        while (($read = $inStream.Read($buffer, 0, $buffer.Length)) -ne 0) {
            $outStream.Write($buffer, 0, $read)
            $total += $read

            $prettyTotal = "{0:N2} MB" -f ($total / 1MB)
            $percent = if ($contentLen) { "{0:N1}%" -f ($total * 100 / $contentLen) } else { "" }

            Write-Host ("`r$ProgressMessage ({1})... {0} {2}" -f $spinner[$i % 4], $prettyTotal, $percent) -NoNewline
            $i++
        }

        Write-Host "`r$DoneMessage!$((' '*40))"
        return $true
    }
    catch {
        Write-Host "`r$ProgressMessage... Failed!" -ForegroundColor Red
        Write-Error "Download failed: $($_.Exception.Message)"
        return $false
    }
    finally {
        if ($outStream) { $outStream.Dispose() }
        if ($inStream) { $inStream.Dispose() }
    }
}

function Test-FileChecksum {
    param(
        [string]$FilePath,
        [string]$ExpectedHash
    )
    
    if (-not $ExpectedHash) {
        Write-Warning "No checksum available for verification"
        return $true
    }
    
    try {
        Write-Verbose "Verifying checksum for $FilePath"
        $actualHash = (Get-FileHash -Path $FilePath -Algorithm SHA256).Hash.ToLower()
        $expectedHash = $ExpectedHash.ToLower().Trim()
        
        if ($actualHash -ne $expectedHash) {
            throw "Checksum verification failed.`nExpected: $expectedHash`nActual:   $actualHash"
        }
        
        Write-Verbose "Checksum verification successful"
        return $true
    }
    catch {
        Write-Error "Checksum verification failed: $($_.Exception.Message)"
        return $false
    }
}

function Install-HelixCli {
    param(
        [object]$AssetInfo,
        [string]$TempDir
    )
    
    # Install as "helix.exe"
    $binaryPath = Join-Path $InstallPath "$BINARY_NAME.exe"
    $tempBinaryPath = Join-Path $TempDir $AssetInfo.FileName
    
    # Create installation directory
    if (-not (Test-Path $InstallPath)) {
        Write-Verbose "Creating installation directory: $InstallPath"
        New-Item -ItemType Directory -Path $InstallPath -Force | Out-Null
    }
    
    # Download binary
    Write-Info "Downloading helix ($([math]::Round($AssetInfo.Size / 1MB, 2)) MB)..."
    if (-not (Start-FileDownload -Url $AssetInfo.DownloadUrl -OutputPath $tempBinaryPath)) {
        throw "Failed to download helix"
    }
    


    # Download and verify checksum if available
    if ($AssetInfo.ChecksumUrl) {
        Write-Verbose "Downloading checksum file"

        try {
            $checksumResp = $http.GetAsync($AssetInfo.ChecksumUrl).GetAwaiter().GetResult()
            $checksumResp.EnsureSuccessStatusCode()

            $expectedHash = $checksumResp.Content.ReadAsStringAsync().GetAwaiter().GetResult() -split '\s+' | Select-Object -First 1

            if (-not (Test-FileChecksum -FilePath $tempBinaryPath -ExpectedHash $expectedHash)) {
                throw "Checksum verification failed"
            }
        }
        catch {
            Write-Warning "Could not verify checksum: $($_.Exception.Message)"
        }
    }
    
    # Install binary
    Write-Verbose "Installing binary to: $binaryPath"
    Move-Item -Path $tempBinaryPath -Destination $binaryPath -Force
    
    return $binaryPath
}

function Update-UserPath {
    param([string]$BinPath)
    
    try {
        # Get current user PATH
        $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
        
        # Check if already in PATH
        $pathEntries = $userPath -split ";" | Where-Object { $_ }
        if ($pathEntries -contains $BinPath) {
            Write-Verbose "PATH already contains $BinPath"
            return $false
        }
        
        # Add to PATH
        $newPath = ($pathEntries + $BinPath) -join ";"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        
        # Update current session
        $env:Path += ";$BinPath"
        
        return $true
    }
    catch {
        Write-Warning "Failed to update PATH: $($_.Exception.Message)"
        return $false
    }
}

function Test-Installation {
    param([string]$BinaryPath)
    
    try {
        if (-not (Test-Path $BinaryPath)) {
            return $false
        }
        
        # Test if binary is executable
        & "$BinaryPath" --version 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
        
        return $false
    }
    catch {
        return $false
    }
}

function Uninstall-HelixCli {
    Write-Info "Uninstalling helix..."
    
    $binaryPath = Join-Path $InstallPath "$BINARY_NAME.exe"
    $removed = $false
    
    # Remove binary
    if (Test-Path $binaryPath) {
        try {
            Remove-Item $binaryPath -Force
            $removed = $true
            Write-Success "Removed binary: $binaryPath"
        }
        catch {
            Write-Error "Failed to remove binary: $($_.Exception.Message)"
        }
    }
    
    # Remove from PATH
    try {
        $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
        $pathEntries = $userPath -split ";" | Where-Object { $_ -and $_ -ne $InstallPath }
        $newPath = $pathEntries -join ";"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        
        if ($userPath -ne $newPath) {
            Write-Success "Removed from PATH"
        }
    }
    catch {
        Write-Warning "Failed to update PATH: $($_.Exception.Message)"
    }
    
    # Remove installation directory if empty
    if (Test-Path $InstallPath) {
        $remainingFiles = Get-ChildItem $InstallPath -File
        if ($remainingFiles.Count -eq 0) {
            try {
                Remove-Item $InstallPath -Force
                Write-Success "Removed empty installation directory"
            }
            catch {
                Write-Warning "Failed to remove installation directory: $($_.Exception.Message)"
            }
        }
    }
    
    if ($removed) {
        Write-Success "helix has been uninstalled"
    }
    else {
        Write-Warning "helix was not found in the installation directory"
    }
}

# Handle parameter preprocessing
if ($Dir) {
    $InstallPath = $Dir
}
else {
    $InstallPath = "$env:USERPROFILE\.local\bin"
}

if ($System) {
    $InstallPath = "$env:ProgramFiles\helix"
}

# Main execution
try {
    # Handle help
    if ($Help) {
        Show-Help
    }
    
    # Handle uninstall
    if ($Uninstall) {
        Uninstall-HelixCli
        exit 0
    }
    
    # Security protocol
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    
    # Welcome message
    Write-ColorOutput "helix Installer v$SCRIPT_VERSION" "Cyan"
    Write-ColorOutput "======================================" "Cyan"
    Write-ColorOutput ""
    
    # Check if running as administrator for system install
    if ($System) {
        $principal = New-Object System.Security.Principal.WindowsPrincipal([System.Security.Principal.WindowsIdentity]::GetCurrent())
        if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
            throw "System install requires administrator privileges. Run PowerShell as Administrator."
        }
        Write-Info "System install directory: $InstallPath"
    }
    
    # Detect architecture
    $architecture = Get-Architecture
    Write-Verbose "Detected architecture: $architecture"
    
    # Get release information
    Write-Verbose "Fetching release information (tag: $Tag)"
    $release = Get-LatestRelease -VersionTag $Tag
    
    Write-Info "Installing helix $($release.tag_name) for Windows $architecture"
    
    # Get asset information
    $assetInfo = Get-AssetInfo -Release $release -Architecture $architecture
    
    # Force install check and version comparison logic
    if (-not $Force) {
        # Check if already installed and same version
        $installedPath = Join-Path $InstallPath "$BINARY_NAME.exe"
        if (Test-Path $installedPath) {
            try {
                $installedVersion = & "$installedPath" --version 2>$null
                if ($installedVersion -match $release.tag_name.TrimStart('v')) {
                    Write-Success "Already up to date!"
                    Write-Info "Use -Force to reinstall"
                    exit 0
                }
            }
            catch {
                Write-Verbose "Could not determine installed version"
            }
        }
    }
    
    # Create temporary directory
    $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) "helix-install-$([Guid]::NewGuid())"
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
    
    try {
        # Install binary
        $binaryPath = Install-HelixCli -AssetInfo $assetInfo -TempDir $tempDir
        
        # Update PATH
        $pathUpdated = Update-UserPath -BinPath $InstallPath
        
        # Verify installation
        Write-Info "Verifying installation..."
        
        if (Test-Installation -BinaryPath $binaryPath) {
            Write-Success "✓ helix $($release.tag_name) installed successfully!"
            Write-Info "Binary location: $binaryPath"
            
            if ($pathUpdated) {
                Write-Success "✓ Added to PATH. You may need to restart your terminal."
            }
            else {
                Write-Info "To use helix, add $InstallPath to your PATH or restart your terminal."
            }
            
            Write-ColorOutput ""
            Write-ColorOutput "Run 'helix --help' to get started!" "Green"
        }
        else {
            throw "Installation verification failed"
        }
    }
    finally {
        # Cleanup
        if (Test-Path $tempDir) {
            Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}
catch {
    $errorMessage = $_.Exception.Message
    
    Write-Error "Installation failed: $errorMessage"
    Write-ColorOutput ""
    Write-ColorOutput "Troubleshooting:" "Yellow"
    Write-ColorOutput "1. Check your internet connection" "White"
    Write-ColorOutput "2. Verify GitHub API rate limits: https://api.github.com/rate_limit" "White"
    Write-ColorOutput "3. Try again later if rate limited" "White"
    Write-ColorOutput "4. Report issues at: https://github.com/$Repo/issues" "White"
    
    exit 1
}

exit 0
