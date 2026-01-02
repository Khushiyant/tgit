$ErrorActionPreference = "Stop"

$Owner = "Khushiyant"
$Repo = "vekt"
$BinaryName = "vekt.exe"
$AssetName = "vekt-windows-amd64.exe"

# Install Directory (Local AppData is standard for user-level installs)
$InstallDir = "$env:LOCALAPPDATA\vekt"
if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir | Out-Null
}

$DownloadUrl = "https://github.com/$Owner/$Repo/releases/latest/download/$AssetName"
$OutputPath = "$InstallDir\$BinaryName"

Write-Host "Downloading vekt..."
Invoke-WebRequest -Uri $DownloadUrl -OutFile $OutputPath

# Add to PATH if not already present
$UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($UserPath -notlike "*$InstallDir*") {
    Write-Host "Adding to PATH..."
    [Environment]::SetEnvironmentVariable("Path", "$UserPath;$InstallDir", "User")
    $env:Path += ";$InstallDir"
    Write-Host "Note: You may need to restart your terminal for PATH changes to take effect."
}

Write-Host "Success! Run 'vekt --help' to get started."