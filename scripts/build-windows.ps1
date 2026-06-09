[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [string]$Runtime = "win-x64",
    [string]$ArtifactsDir
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $PSCommandPath
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
$projectPath = Join-Path $rootDir "BackupsterAgent\BackupsterAgent.csproj"
$publishDir = Join-Path $rootDir "publish\$Runtime"

if ([string]::IsNullOrWhiteSpace($ArtifactsDir)) {
    $ArtifactsDir = Join-Path $rootDir "artifacts"
}

function Get-MsBuildProperty {
    param([Parameter(Mandatory = $true)][string]$Name)

    $value = & dotnet msbuild $projectPath -nologo "-getProperty:$Name"
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet msbuild failed while reading $Name"
    }

    return ($value | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 1).Trim()
}

$version = Get-MsBuildProperty -Name "Version"
if ([string]::IsNullOrWhiteSpace($version)) {
    throw "Project Version is empty: $projectPath"
}

Write-Host "Building BackupsterAgent $version for $Runtime..."

if (Test-Path -LiteralPath $publishDir) {
    Remove-Item -LiteralPath $publishDir -Recurse -Force
}

New-Item -ItemType Directory -Path $publishDir -Force | Out-Null
New-Item -ItemType Directory -Path $ArtifactsDir -Force | Out-Null

& dotnet publish $projectPath `
    -c $Configuration `
    -r $Runtime `
    --self-contained true `
    -p:PublishSingleFile=true `
    "-p:Version=$version" `
    "-p:InformationalVersion=$version" `
    -o $publishDir

if ($LASTEXITCODE -ne 0) {
    throw "dotnet publish failed"
}

$zipPath = Join-Path $ArtifactsDir "BackupsterAgent-v$version-$Runtime.zip"
if (Test-Path -LiteralPath $zipPath) {
    Remove-Item -LiteralPath $zipPath -Force
}

Compress-Archive -Path (Join-Path $publishDir "*") -DestinationPath $zipPath
Write-Host "Created $zipPath"
