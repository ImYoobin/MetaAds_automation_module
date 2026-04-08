param(
    [string]$BuildRoot = "C:\MetaExportBuild",
    [switch]$CleanVenv
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

Write-Warning "build_exe.ps1 is deprecated. Use .\build_meta_release.ps1 directly."

$args = @("-BuildRoot", $BuildRoot)
if ($CleanVenv) {
    $args += "-CleanVenv"
}

& (Join-Path $PSScriptRoot "build_meta_release.ps1") @args
