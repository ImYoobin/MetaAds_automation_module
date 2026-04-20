# Meta Build Scripts

Run from `module_source/meta_automation_module`.

## Build
```powershell
.\build\meta\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild"
```

Optional clean venv:
```powershell
.\build\meta\build_meta_release.ps1 -BuildRoot "C:\MetaExportBuild" -CleanVenv
```

## Inputs
- `launcher.py`
- `app/main.py`
- `.streamlit/config.toml`
- `config/meta/**`
- `dashboard/**`
- `meta_core/**`
- `meta_history_log/**`
- `RUN.md`

## Outputs
- `dist\Meta_Export\Meta_Export.exe`
- `dist\Meta_Export_Debug\Meta_Export_Debug.exe`
- `dist\Meta_Export\Meta_Export.exe`
- `dist\Meta_Export_Debug\Meta_Export_Debug.exe`
- mirrored runtime in `release_source\meta_automation`

## Notes
- `meta_history_log` is bundled into the single Meta runtime and no longer ships as a separate EXE.
- The release mirror is refreshed from the final built runtime, so obsolete files such as `Meta_History_Log.exe` are removed automatically.
- ZIP archives are no longer produced during the Meta release build.
