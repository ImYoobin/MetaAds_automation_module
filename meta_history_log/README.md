# Meta_history_log

Independent weekly/manual runner for Meta Ads Manager **Ad Sets Activity History** collection.

## Scope
- Reads shared config only:
  - `config/meta/activity_catalog.json`
  - `config/meta/runtime_settings.json`
- No target override in v1.
- Targets are derived from enabled report URLs (`act`, `business_id`).

## Collection Strategy (v1.1)
- Primary: URL bootstrap via `manage/campaigns` + `filter_set` for `{activity_prefix}_`.
- Verification source of truth: filter **chip** text in UI.
  - chip definition: `Campaign name contains all of {activity_prefix}_` (EN/KR regex)
- Fallback order:
1. UI suggestion click (`Campaign name contains all of ...`)
2. Name/popup force set (field/operator/value)
3. Headful only: wait for manual user filter action and auto-resume when chip is detected
- In history panel, always force by UI:
1. `Last 7 days`
2. `Activity history: Ad Sets`

## Output
- File per activity prefix:
  - `{activity_prefix}_history_{yyyymmdd}.xlsx`
- Output root:
  - `{runtime_settings.output_dir}\history_logs\{yyyymmdd}`
- If not writable, fallback:
  - `meta_history_log/_local_output/history_logs/{yyyymmdd}`
- Zero rows still produce valid xlsx headers.

## Install
```powershell
cd .\meta_history_log
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Optional browser install:
```powershell
# Only needed when runtime browser is 'chromium'
python -m playwright install chromium
```

## Run
```powershell
cd .\meta_history_log
python .\main.py --verbose
```

Optional:
```powershell
python .\main.py --dry-run
python .\main.py --config .\config.yaml
python .\main.py --verbose --force-ui-filter-fallback
```

## Runtime keys (runtime_settings.json)
- `history_browser` (`msedge` | `chrome` | `chromium`)
- `history_headless` (bool)
- `history_user_data_dir` (path)

Defaults:
- browser: `msedge`
- headless: `false`
- profile: `%USERPROFILE%\MetaAdsExport\user_data\meta\<browser>`

## Logging and Failure Artifacts
- Logs:
  - `{output_dir}\history_logs\{yyyymmdd}\logs\meta_history_log_*.log`
- Failure screenshots:
  - `{output_dir}\history_logs\{yyyymmdd}\screenshots\*.png`
- Diagnostics include:
  - `shell_text_found`
  - `search_role_count`
  - `input_combobox_count`
  - `progress_visible`
  - `active_element_meta`

## Isolation from report export
- Independent runtime/entrypoint (no coupling to report export flow)
- Shared only via config files under `config/meta`
- Shared browser profile default to reuse login across report/action-log phases
- No use of URL hacks for `selected_adset_ids`/history scope control

## Failure Policy
- On first account failure, stop whole run (non-zero exit code)
- Preserve files already saved for completed activities
- Save current activity output with collected rows before stop

## PyInstaller (release)
```powershell
cd .\meta_history_log
pyinstaller --noconfirm --onefile --name Meta_history_log .\main.py
```
