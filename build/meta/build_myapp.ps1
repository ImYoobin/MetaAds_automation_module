param(
    [string]$BuildRoot = "C:\MetaExportBuild",
    [switch]$CleanVenv
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($env:META_ALLOW_LEGACY_BUILD -ne "1") {
    throw "build_myapp.ps1 is deprecated. Use .\build_meta_release.ps1 (set META_ALLOW_LEGACY_BUILD=1 to bypass)."
}

$args = @("-BuildRoot", $BuildRoot)
if ($CleanVenv) {
    $args += "-CleanVenv"
}

& (Join-Path $PSScriptRoot "build_meta_release.ps1") @args
