# Meta Automation Run Guide

## 1) Local install (dev run)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 2) Local launch (single user entrypoint)
```powershell
python main.py
```

If browser does not open automatically, open `http://localhost:8502`.

## 3) Official EXE build (Meta-only, Streamlit 유지)
Build must run outside OneDrive sync path.

```powershell
# Optional: force clean venv rebuild
.\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild" -CleanVenv

# Fast path (reuses build venv)
.\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild"
```

What this does:
1. Stage only required modules (`dashboard`, `meta_core`, launcher, app/config)
2. Build launcher EXE in onedir mode (`MyApp.exe`, `MyAppDebug.exe`)
3. Bundle portable Python runtime under `_internal\python_runtime`
4. Run app via launcher: `python -m streamlit run app/main.py`
5. Produce final artifacts in repo `dist/` and `release/`

Outputs:
- `dist\MyApp\MyApp.exe` (final, noconsole)
- `dist\MyAppDebug\MyAppDebug.exe` (debug, console)
- `release\MyApp_win.zip`
- `release\MyAppDebug_win.zip`

## 4) Final package runtime layout
`MyApp_win.zip` contains:
- `MyApp.exe`
- `_internal\...` (launcher runtime + python_runtime)
- `app\main.py`
- `.streamlit\config.toml`
- `dashboard\...`
- `meta_core\...`
- `config\meta\activity_catalog.json`
- `config\meta\activity_catalog.example.json`
- `logs\`
- `RUN.md`

## 5) User run flow
1. Unzip `MyApp_win.zip`
2. Double-click `MyApp.exe`
3. Browser opens `http://localhost:8502`
4. Use dashboard for Meta login -> export -> unified workbook

If launch fails, check:
- `logs\launcher_*.log`
- `logs\streamlit_stderr.log`

## 6) Deprecated build paths
- `build_exe.ps1`: deprecated wrapper (delegates to `build_meta_release.ps1`)
- `build_myapp.ps1`: deprecated legacy flow (disabled unless `META_ALLOW_LEGACY_BUILD=1`)
- `meta_ads_auto_export_release.spec`: deprecated for official release build
