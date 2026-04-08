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
- `config/meta/activity_catalog*.json`
- `dashboard/**`
- `meta_core/**`
