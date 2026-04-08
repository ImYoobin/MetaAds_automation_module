# Meta Automation Run Guide (Module SSOT)

`module_source/meta_automation_module` is the source of truth.
`release_source/meta_automation` is a mirrored runtime target.

## 1) Local install (dev run, from module root)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r .\build\meta\requirements.meta.runtime.txt
```

## 2) Local launch (actual entrypoint)
```powershell
python -m streamlit run .\app\main.py
```

If browser does not open automatically, open `http://localhost:8502`.

## 3) Sync module -> release (from workspace root)
```powershell
.\tools\meta_sync.ps1
.\tools\meta_parity_check.ps1
```

Equivalent commands from module root:
```powershell
..\..\tools\meta_sync.ps1
..\..\tools\meta_parity_check.ps1
```

Managed mirror scope:
- `.streamlit/`
- `app/`
- `config/meta/`
- `dashboard/`
- `meta_core/`
- `RUN.md`

Excluded from sync:
- `_internal/`
- `logs/`
- `MyApp.exe`
- build artifacts

## 4) Official EXE build (from module root)
Build outside OneDrive/repo path.

```powershell
# Optional: force clean venv rebuild
.\build\meta\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild" -CleanVenv

# Fast path (reuses build venv)
.\build\meta\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild"
```

Outputs:
- `dist\MyApp\MyApp.exe` (final, noconsole)
- `dist\MyAppDebug\MyAppDebug.exe` (debug, console)
- `release\MyApp_win.zip`
- `release\MyAppDebug_win.zip`

## 5) Smoke test
```powershell
.\dist\MyApp\MyApp.exe
```

Check logs:
- `logs\launcher_*.log`
- `logs\streamlit_stderr.log`

Expected launcher flow:
1. `streamlit_cmd=[..., "-m", "streamlit", "run", "...\app\main.py", ... "--server.port=8502", ...]`
2. `streamlit_ready host=127.0.0.1 port=8502`
3. `browser_opened url=http://localhost:8502`

## 6) Deprecated build-kit path
- `module_source\_build_kit\meta\*.ps1` remains as compatibility wrappers.
- Preferred path: `module_source\meta_automation_module\build\meta\*.ps1`.
