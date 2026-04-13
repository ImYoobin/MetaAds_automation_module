# Meta_history_log (history_source)

Independent weekly manual runner for Meta Ads Manager **Ad Sets Activity History** collection.

## Scope
- Runs from `history_source` first (safe staging area).
- Reads shared config format only:
  - `history_source/config/meta/activity_catalog.json`
  - `history_source/config/meta/runtime_settings.json`
- No target override in v1.  
  Targets are always derived from report URLs (`act`, `business_id`).

## Collection Strategy (v1.1)
- Entry URL uses `manage/campaigns` with bootstrap `filter_set` for `{activity_prefix}_`.
- Bootstrap filter is always validated by UI chip text (`Campaign name contains all of {activity_prefix}_`).
- `chip` means the active filter tag shown in search bar (`Edit filter/필터 수정` button text).
- If validation fails, script falls back to UI filtering:
1. Type `{activity_prefix}_` and click suggested `Campaign name contains all of ...`
2. If still needed, open Name/popup and force field/operator/value.
- If automatic UI fallback also fails:
1. Headful: wait indefinitely for user to click/apply filter manually, then auto-resume when chip is detected.
2. Headless: restart browser context as headful once, then retry same account.
- In history panel, script always forces UI settings:
1. `Last 7 days`
2. `Activity history: Ad Sets`

## Output
- Per activity prefix file:
  - `{activity_prefix}_history_{yyyymmdd}.xlsx`
- Output root:
  - `{runtime_settings.output_dir}\\history_logs\\{yyyymmdd}`
- If that path is not writable, script falls back to:
  - `history_source/meta_history_log/_local_output/history_logs/{yyyymmdd}`
- Empty result is still saved as a valid xlsx with headers.

## Files
- `main.py`
- `requirements.txt`
- `config.example.yaml` (optional runtime/path overrides only)

## Install
```powershell
cd .\history_source\meta_history_log
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Optional browser install:
```powershell
# Only needed when runtime browser is `chromium`
python -m playwright install chromium
```

If `history_browser` is `chrome` or `msedge`, installed local browser can be reused.

## Run
```powershell
cd .\history_source\meta_history_log
python .\main.py
```

Real-browser smoke test (headful):
```powershell
cd "C:\Users\im yoobin\Downloads\ads_automation_source\history_source\meta_history_log"
.\.venv\Scripts\Activate.ps1
python .\main.py
```

Optional:
```powershell
# validate config/targets only
python .\main.py --dry-run

# use external config yaml/json
python .\main.py --config .\config.yaml

# force UI filter fallback path (manual verification mode)
python .\main.py --verbose --force-ui-filter-fallback
```

## Runtime Option Keys (from runtime_settings.json)
Supported optional keys:
- `history_browser` (`msedge` | `chrome` | `chromium`)
- `history_headless` (bool)
- `history_user_data_dir` (path)

Fallbacks:
- browser: `browser` key -> `msedge`
- headless: `false`
- user profile: `%USERPROFILE%\MetaAdsExport\user_data\meta_history_log`

## Logging and Failure Artifacts
- Logs:
  - `{output_dir}\history_logs\{yyyymmdd}\logs\meta_history_log_*.log`
- Screenshots on failure:
  - `{output_dir}\history_logs\{yyyymmdd}\screenshots\*.png`
- Each failed stage logs step name and account context.
- Filter readiness diagnostics include:
  - `shell_text_found`
  - `search_role_count`
  - `input_combobox_count`
  - `progress_visible`
  - `active_element_meta`

## Selector Strategy
- EN/KR dual support.
- Role/aria selectors first, text/regex fallback.
- URL params `selected_adset_ids` and `columns` are not used.
- URL `filter_set` is bootstrap-only (final correctness is UI-verified).
- Activity History button handles dual aria-label state (disabled/enabled).
- History table uses `table[role='grid'][aria-label='Activity log table|활동 로그 테이블']`.

## Failure Policy
- On first account failure, stop the whole run immediately (non-zero exit code).
- Keep files already saved for completed activities.
- Save current activity output with rows collected before failure.
- When run completes, output folder opens automatically in Windows File Explorer.

## High-Change UI Points (Maintenance Watchlist)
- Filter popup container (`uiContextualLayer`) and combobox order.
- History entry button aria-label wording/state transitions.
- Date picker preset radio layout (`value='last_7d'` can appear in multiple groups).
- Scope dropdown labels (`Activity history` / `활동 기록`).
- History table aria-label and lazy-load behavior.
- Locale text changes (EN/KR) for menu items and buttons.

## Promotion Workflow
1. Develop and validate in `history_source`.
2. Confirm at least one real successful run using Python script.
3. Move final code to `module_source`.
4. Smoke test again.
5. Build independent exe in `release_source` once (final release step).

## PyInstaller (Final release only)
Run only after script behavior is verified:
```powershell
cd .\history_source\meta_history_log
pyinstaller --noconfirm --onefile --name Meta_history_log .\main.py
```
