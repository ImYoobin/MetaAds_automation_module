param(
    [string]$BuildRoot = "C:\MetaExportBuild",
    [switch]$CleanVenv
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$workspaceRoot = (Resolve-Path (Join-Path $repoRoot "..\..")).Path
Set-Location $repoRoot

$workspace = Join-Path $BuildRoot "meta_auto_export_build"
$stageRoot = Join-Path $workspace "stage_src"
$pyiDistRoot = Join-Path $workspace ".tmp_pyinstaller_dist"
$pyiWorkRoot = Join-Path $workspace ".tmp_pyinstaller_work"
$venvDir = Join-Path $workspace ".venv_pack"
$pipCache = Join-Path $workspace ".pip_cache"
$tmpDir = Join-Path $workspace ".tmp"

$finalAppName = "Meta_Export"
$debugAppName = "Meta_Export_Debug"
$repoDistFinal = Join-Path $repoRoot "dist\$finalAppName"
$repoDistDebug = Join-Path $repoRoot "dist\$debugAppName"
$repoReleaseDir = Join-Path $repoRoot "release"
$repoReleaseZip = Join-Path $repoReleaseDir "${finalAppName}_win.zip"
$repoReleaseZipDebug = Join-Path $repoReleaseDir "${debugAppName}_win.zip"
$repoBuildLog = Join-Path $repoReleaseDir "build_release.log"
$releaseMirrorRoot = Join-Path $workspaceRoot "release_source\meta_automation"
$runtimeRequirements = (Resolve-Path (Join-Path $PSScriptRoot "requirements.meta.runtime.txt")).Path

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Message)
    $line = "[build-meta] $Message"
    Write-Host $line
    Add-Content -LiteralPath $repoBuildLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $line"
}

function Ensure-Dir {
    param([Parameter(Mandatory = $true)][string]$Path)
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Copy-DirectoryMirror {
    param(
        [Parameter(Mandatory = $true)][string]$From,
        [Parameter(Mandatory = $true)][string]$To,
        [string[]]$ExcludeDirs = @(),
        [string[]]$ExcludeFiles = @()
    )

    if (!(Test-Path -LiteralPath $From)) {
        throw "Source not found: $From"
    }

    Ensure-Dir -Path $To

    $args = @(
        $From,
        $To,
        "/MIR",
        "/R:2",
        "/W:1",
        "/NFL",
        "/NDL",
        "/NJH",
        "/NJS",
        "/NP"
    )
    if ($ExcludeDirs.Count -gt 0) {
        $args += "/XD"
        $args += $ExcludeDirs
    }
    if ($ExcludeFiles.Count -gt 0) {
        $args += "/XF"
        $args += $ExcludeFiles
    }

    $null = robocopy @args
    if ($LASTEXITCODE -gt 7) {
        throw "robocopy failed: $From -> $To (exit=$LASTEXITCODE)"
    }
}

function Ensure-CommandPython {
    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        return "python"
    }
    $pyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCommand) {
        return "py -3"
    }
    throw "Python launcher not found. Install Python 3.12+ and retry."
}

function Ensure-BuildVenv {
    param([Parameter(Mandatory = $true)][string]$VenvPath)

    if ($CleanVenv -and (Test-Path -LiteralPath $VenvPath)) {
        Write-Step "Cleaning build venv"
        Remove-Item -LiteralPath $VenvPath -Recurse -Force -ErrorAction SilentlyContinue
    }

    if (!(Test-Path -LiteralPath $VenvPath)) {
        Write-Step "Creating build venv: $VenvPath"
        $pyCmd = Ensure-CommandPython
        $venvCreated = $false
        if ($pyCmd -eq "python") {
            & python -m venv $VenvPath
            if ($LASTEXITCODE -eq 0) { $venvCreated = $true }
        }
        else {
            & py -3 -m venv $VenvPath
            if ($LASTEXITCODE -eq 0) { $venvCreated = $true }
        }

        if (-not $venvCreated) {
            Write-Step "python -m venv failed, retrying with virtualenv"
            if ($pyCmd -eq "python") {
                & python -m virtualenv $VenvPath
            }
            else {
                & py -3 -m virtualenv $VenvPath
            }
            if ($LASTEXITCODE -eq 0) { $venvCreated = $true }
        }

        if (-not $venvCreated) {
            throw "Failed to create build venv."
        }
    }
}

function Install-BuildDependencies {
    param(
        [Parameter(Mandatory = $true)][string]$VenvPython,
        [Parameter(Mandatory = $true)][string]$RequirementsPath
    )

    Write-Step "Upgrading pip"
    & $VenvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to upgrade pip."
    }

    Write-Step "Installing runtime/build deps"
    & $VenvPython -m pip install -r $RequirementsPath pyinstaller pyinstaller-hooks-contrib
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install dependencies."
    }
}

function Invoke-LauncherBuild {
    param(
        [Parameter(Mandatory = $true)][string]$PyInstallerPath,
        [Parameter(Mandatory = $true)][string]$LauncherPath,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][bool]$Console
    )

    Write-Step "Building launcher: $Name (console=$Console)"

    Remove-Item -LiteralPath (Join-Path $pyiDistRoot $Name) -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $pyiWorkRoot $Name) -Recurse -Force -ErrorAction SilentlyContinue

    $args = @(
        "--noconfirm",
        "--clean",
        "--onedir",
        "--name", $Name,
        "--distpath", $pyiDistRoot,
        "--workpath", $pyiWorkRoot,
        "--specpath", $workspace,
        "--exclude-module", "pytest",
        "--exclude-module", "unittest",
        "--exclude-module", "IPython",
        "--exclude-module", "jupyter",
        "--exclude-module", "tensorflow",
        "--exclude-module", "torch",
        "--exclude-module", "langchain",
        $LauncherPath
    )

    if ($Console) {
        $args += "--console"
    }
    else {
        $args += "--noconsole"
    }

    & $PyInstallerPath @args
    if ($LASTEXITCODE -ne 0) {
        throw "PyInstaller failed for $Name"
    }
}

function Remove-HeavyPackages {
    param([Parameter(Mandatory = $true)][string]$SitePackagesPath)

    $patterns = @(
        "tensorflow*",
        "torch*",
        "langchain*",
        "langsmith*",
        "triton*"
    )

    foreach ($pattern in $patterns) {
        Get-ChildItem -Path $SitePackagesPath -Filter $pattern -Force -ErrorAction SilentlyContinue |
            ForEach-Object {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
            }
    }
}

function Stage-FinalPayload {
    param(
        [Parameter(Mandatory = $true)][string]$FinalRoot,
        [Parameter(Mandatory = $true)][string]$VenvPython,
        [Parameter(Mandatory = $true)][string]$VenvSitePackages
    )

    $basePythonHome = (& $VenvPython -c "import os,sys; print(os.path.abspath(sys.base_prefix))").Trim()
    if (!(Test-Path -LiteralPath $basePythonHome)) {
        throw "Failed to resolve base python home: $basePythonHome"
    }

    $runtimeRoot = Join-Path $FinalRoot "_internal\python_runtime"
    $runtimeSitePackages = Join-Path $runtimeRoot "Lib\site-packages"

    Write-Step "Staging base python runtime"
    Copy-DirectoryMirror -From $basePythonHome -To $runtimeRoot

    Write-Step "Overlaying site-packages"
    Copy-DirectoryMirror -From $VenvSitePackages -To $runtimeSitePackages

    Write-Step "Pruning heavy modules"
    Remove-HeavyPackages -SitePackagesPath $runtimeSitePackages

    Write-Step "Copying app modules"
    Copy-DirectoryMirror -From (Join-Path $stageRoot "dashboard") -To (Join-Path $FinalRoot "dashboard") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
    Copy-DirectoryMirror -From (Join-Path $stageRoot "meta_core") -To (Join-Path $FinalRoot "meta_core") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
    Copy-DirectoryMirror -From (Join-Path $stageRoot "meta_history_log") -To (Join-Path $FinalRoot "meta_history_log") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")

    Ensure-Dir -Path (Join-Path $FinalRoot "app")
    Ensure-Dir -Path (Join-Path $FinalRoot ".streamlit")
    Ensure-Dir -Path (Join-Path $FinalRoot "config\meta")
    Ensure-Dir -Path (Join-Path $FinalRoot "logs")

    Copy-Item -LiteralPath (Join-Path $stageRoot "app\main.py") -Destination (Join-Path $FinalRoot "app\main.py") -Force
    Copy-Item -LiteralPath (Join-Path $stageRoot ".streamlit\config.toml") -Destination (Join-Path $FinalRoot ".streamlit\config.toml") -Force
    Copy-DirectoryMirror -From (Join-Path $stageRoot "config\meta") -To (Join-Path $FinalRoot "config\meta") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
    Copy-Item -LiteralPath (Join-Path $stageRoot "RUN.md") -Destination (Join-Path $FinalRoot "RUN.md") -Force
}

function Assert-RequiredSource {
    $required = @(
        "launcher.py",
        "app\main.py",
        ".streamlit\config.toml",
        "config\meta",
        "config\meta\activity_catalog.json",
        "config\meta\activity_catalog.example.json",
        "config\meta\runtime_settings.json",
        "dashboard",
        "meta_core",
        "meta_history_log",
        "RUN.md"
    )
    foreach ($item in $required) {
        $path = Join-Path $repoRoot $item
        if (!(Test-Path -LiteralPath $path)) {
            throw "Required source missing: $path"
        }
    }
}

function Prepare-StageSource {
    Write-Step "Preparing stage source"
    Remove-Item -LiteralPath $stageRoot -Recurse -Force -ErrorAction SilentlyContinue
    Ensure-Dir -Path $stageRoot

    Copy-Item -LiteralPath (Join-Path $repoRoot "launcher.py") -Destination (Join-Path $stageRoot "launcher.py") -Force

    Ensure-Dir -Path (Join-Path $stageRoot "app")
    Ensure-Dir -Path (Join-Path $stageRoot ".streamlit")
    Ensure-Dir -Path (Join-Path $stageRoot "config\meta")

    Copy-Item -LiteralPath (Join-Path $repoRoot "app\main.py") -Destination (Join-Path $stageRoot "app\main.py") -Force
    Copy-Item -LiteralPath (Join-Path $repoRoot ".streamlit\config.toml") -Destination (Join-Path $stageRoot ".streamlit\config.toml") -Force
    Copy-Item -LiteralPath $runtimeRequirements -Destination (Join-Path $stageRoot "requirements.meta.runtime.txt") -Force
    Copy-Item -LiteralPath (Join-Path $repoRoot "RUN.md") -Destination (Join-Path $stageRoot "RUN.md") -Force

    Copy-DirectoryMirror -From (Join-Path $repoRoot "config\meta") -To (Join-Path $stageRoot "config\meta") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
    Copy-DirectoryMirror -From (Join-Path $repoRoot "dashboard") -To (Join-Path $stageRoot "dashboard") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
    Copy-DirectoryMirror -From (Join-Path $repoRoot "meta_core") -To (Join-Path $stageRoot "meta_core") -ExcludeDirs @("legacy", "__pycache__") -ExcludeFiles @("*.pyc", "__main__.py")
    Copy-DirectoryMirror -From (Join-Path $repoRoot "meta_history_log") -To (Join-Path $stageRoot "meta_history_log") -ExcludeDirs @("__pycache__") -ExcludeFiles @("*.pyc")
}

Ensure-Dir -Path $repoReleaseDir
Set-Content -LiteralPath $repoBuildLog -Value ""
Write-Step "Build start"
Write-Step "Repo root: $repoRoot"
Write-Step "Workspace root: $workspaceRoot"
Write-Step "Build root: $BuildRoot"

$repoRootAbs = [System.IO.Path]::GetFullPath($repoRoot)
$buildRootAbs = [System.IO.Path]::GetFullPath($BuildRoot)
if ($buildRootAbs.StartsWith($repoRootAbs, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "BuildRoot must be outside repo path. Current: $buildRootAbs"
}

Assert-RequiredSource

Ensure-Dir -Path $BuildRoot
Ensure-Dir -Path $workspace
Ensure-Dir -Path $pipCache
Ensure-Dir -Path $tmpDir
Ensure-Dir -Path $pyiDistRoot
Ensure-Dir -Path $pyiWorkRoot

$env:TEMP = $tmpDir
$env:TMP = $tmpDir
$env:TMPDIR = $tmpDir
$env:PIP_CACHE_DIR = $pipCache

Prepare-StageSource

Ensure-BuildVenv -VenvPath $venvDir

$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvPyInstaller = Join-Path $venvDir "Scripts\pyinstaller.exe"
$venvSitePackages = Join-Path $venvDir "Lib\site-packages"

if (!(Test-Path -LiteralPath $venvPython)) {
    throw "Missing venv python: $venvPython"
}

Install-BuildDependencies -VenvPython $venvPython -RequirementsPath (Join-Path $stageRoot "requirements.meta.runtime.txt")

if (!(Test-Path -LiteralPath $venvPyInstaller)) {
    throw "Missing pyinstaller: $venvPyInstaller"
}

Write-Step "Cleaning old outputs"
Remove-Item -LiteralPath $repoDistFinal -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $repoDistDebug -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $repoReleaseZip -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $repoReleaseZipDebug -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath (Join-Path $workspace "$finalAppName.spec") -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath (Join-Path $workspace "$debugAppName.spec") -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath (Join-Path $workspace "$debugAppName.exe.tmp") -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath (Join-Path $pyiDistRoot $finalAppName) -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath (Join-Path $pyiDistRoot $debugAppName) -Recurse -Force -ErrorAction SilentlyContinue

Invoke-LauncherBuild -PyInstallerPath $venvPyInstaller -LauncherPath (Join-Path $stageRoot "launcher.py") -Name $finalAppName -Console:$false
Invoke-LauncherBuild -PyInstallerPath $venvPyInstaller -LauncherPath (Join-Path $stageRoot "launcher.py") -Name $debugAppName -Console:$true

$finalBuiltRoot = Join-Path $pyiDistRoot $finalAppName
$debugBuiltRoot = Join-Path $pyiDistRoot $debugAppName
if (!(Test-Path -LiteralPath $finalBuiltRoot)) {
    throw "Final launcher output missing: $finalBuiltRoot"
}
if (!(Test-Path -LiteralPath $debugBuiltRoot)) {
    throw "Debug launcher output missing: $debugBuiltRoot"
}

$debugExeTmp = Join-Path $workspace "$debugAppName.exe.tmp"
Copy-Item -LiteralPath (Join-Path $debugBuiltRoot "$debugAppName.exe") -Destination $debugExeTmp -Force

Stage-FinalPayload -FinalRoot $finalBuiltRoot -VenvPython $venvPython -VenvSitePackages $venvSitePackages

Copy-DirectoryMirror -From $finalBuiltRoot -To $debugBuiltRoot
if (Test-Path -LiteralPath (Join-Path $debugBuiltRoot "$finalAppName.exe")) {
    Remove-Item -LiteralPath (Join-Path $debugBuiltRoot "$finalAppName.exe") -Force
}
Move-Item -LiteralPath $debugExeTmp -Destination (Join-Path $debugBuiltRoot "$debugAppName.exe") -Force

Write-Step "Copying final artifacts to repo dist"
Copy-DirectoryMirror -From $finalBuiltRoot -To $repoDistFinal
Copy-DirectoryMirror -From $debugBuiltRoot -To $repoDistDebug

Write-Step "Mirroring final runtime to release source"
Copy-DirectoryMirror -From $finalBuiltRoot -To $releaseMirrorRoot

Write-Step "Build complete"
Write-Host "  Final EXE: $repoDistFinal\$finalAppName.exe"
Write-Host "  Debug EXE: $repoDistDebug\$debugAppName.exe"
Write-Host "  Release mirror: $releaseMirrorRoot"
Write-Host "  Build log: $repoBuildLog"
