# Meta Automation Run Guide (Module SSOT)

`module_source/meta_automation_module` is the source of truth.
`release_source/meta_automation` is the mirrored runtime target.

## 1) Local install (dev run, from module root)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r .\build\meta\requirements.meta.runtime.txt
```

## 2) Local launch (single UI entrypoint)
```powershell
python -m streamlit run .\app\main.py
```

If browser does not open automatically, open `http://localhost:8502`.

The integrated UI supports two execution options:
- `캠페인 데이터 다운로드`
- `액션 로그 다운로드`

When both are enabled, the app always runs:
1. report download for all selected activities
2. action-log download for all selected activities

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
- `meta_history_log/`
- `RUN.md`

Excluded from sync:
- `_internal/`
- `logs/`
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
- `dist\Meta_Export\Meta_Export.exe` (final, noconsole)
- `dist\Meta_Export_Debug\Meta_Export_Debug.exe` (debug, console)
- `dist\Meta_Export\Meta_Export.exe`
- `dist\Meta_Export_Debug\Meta_Export_Debug.exe`
- `release_source\meta_automation\Meta_Export.exe`

`Meta_History_Log.exe` is no longer produced or shipped.
ZIP archives are no longer produced during the Meta release build.

## 5) Smoke test
```powershell
.\dist\Meta_Export\Meta_Export.exe
```

Check logs:
- `logs\launcher_*.log`
- `logs\streamlit_stderr.log`

Expected launcher flow:
1. `streamlit_cmd=[..., "-m", "streamlit", "run", "...\app\main.py", ... "--server.port=8502", ...]`
2. `streamlit_ready host=127.0.0.1 port=8502`
3. `browser_opened url=http://localhost:8502`

Expected UI smoke:
1. 실행 옵션 영역에 `캠페인 데이터 다운로드` / `액션 로그 다운로드` 체크박스가 보인다.
2. 둘 다 켜면 report 단계가 모두 끝난 뒤 history 단계가 시작된다.
3. 하단 진행 영역에 report와 action-log 결과가 별도 섹션으로 표시된다.

## 6) Deprecated build-kit path
- `module_source\_build_kit\meta\*.ps1` remains as compatibility wrappers.
- Preferred path: `module_source\meta_automation_module\build\meta\*.ps1`.

## 7) Integrated action-log runtime notes
`meta_history_log` is now bundled into the same runtime and imported by the Streamlit dashboard.

Shared config paths:
- `config/meta/activity_catalog.json`
- `config/meta/runtime_settings.json`

Default history runtime paths:
- browser profile: `%USERPROFILE%\MetaAdsExport\user_data\meta\<browser>`
- action-log output: `%USERPROFILE%\MetaAdsExport\output\action_log\{yyyymmdd}`
- trace: `%USERPROFILE%\MetaAdsExport\trace\{yyyymmdd}`
