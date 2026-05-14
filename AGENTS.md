# Meteo Analyst

Weather station analysis system with LLM-powered vision classification. Runs on a Proxmox LXC with Home Assistant integration. Serves a real-time dashboard (`/dashboard`) consumed by a minimal Android WebView app ("Datmet").

## Deployment paths

| What | Path |
|------|------|
| Code (git root) | `/opt/meteo-analyst/` |
| Virtualenv | `/opt/meteo-analyst/venv/` |
| Env file | `/opt/meteo-analyst/.env` (loaded by every script) |
| SQLite DB | `/data/meteo/meteo.db` (mountbind persistent) |
| Photos | `/data/meteo/{station}/YYYYMMDD/snapshot_HHMMSS.jpg` |
| Logs | `/var/log/meteo-*.log` |
| Systemd unit | `meteo-api.service` |

## Scripts

| Script | Purpose | Trigger |
|--------|---------|---------|
| `meteo_analyst.py` | LLM vision analysis of latest.jpg | cron (every capture) or `--force` |
| `meteo_api.py` | Flask REST API + dashboard (port 8765) | systemd service |
| `meteo_collector.py` | Sensor poll (HA API or Ecowitt cloud) | 2× cron entries (one per station) |
| `meteo_sky_classifier.py` | Detailed sky classification (bulk) | manual |
| `meteo_resum.py` | Daily narrative summary | cron (daily) |
| `meteo_analisi_periode.py` | Multi-day period analysis | manual |
| `meteo_ecowitt_history.py` | Historical Ecowitt data from cloud API | manual |
| `meteo_recalcula_sensors.py` | Backfill sensor data for sky_classifications | manual |
| `meteo_solar.py` | Solar position (alba/posta) via `astral` lib | imported by API |
| `scripts/capture.sh` | Camera capture from 192.168.31.182 | cron (every 2-5 min) |

## Stations

- `torrelles` (default) — HA source (`192.168.31.228:8123`), MAC `94:3C:C6:41:38:9F`
- `espui` — Ecowitt cloud API source, MAC `88:13:BF:46:4D:43`

Each station has its own sensor configuration (`meteo_collector.py:28-63`). Torrelles polls Home Assistant; Espui calls Ecowitt cloud directly.

## LLM providers

`claude` (default), `openai`, `gemini`, `local` (Ollama). Set via `METEO_PROVIDER` or `--provider` flag.

Default models: `claude-haiku-4-5-20251001`, `gpt-4o-mini`, `gemini-2.0-flash`, `llava`.

Override per-provider with `METEO_MODEL_CLAUDE`, `METEO_MODEL_OPENAI`, etc.

Two env vars control the default: `METEO_PROVIDER` (used by `meteo_providers.py` via `get_provider()`) and `METEO_PROVIDER_PROD` (used by `meteo_analyst.py` and `meteo_api.py`). They can differ — the API only returns analyses from `METEO_PROVIDER_PROD`.

## Shared module

All scripts import from `meteo_providers` via `sys.path.insert(0, "/opt/meteo-analyst")`. The module exposes `get_provider()`, `llm_vision()`, `llm_text()`.

## Database tables

- `analisis` — LLM vision analysis (what meteo_analyst.py writes)
- `meteo_readings` — Ecowitt sensor readings incl. lightning & CH1 (what meteo_collector.py writes)
- `sky_classifications` — detailed per-photo sky classification
- `resums` — daily narrative summaries

## API endpoints (port 8765)

Legacy endpoints (HA integration):
- `GET /meteo/latest` — last vision analysis (filtered by PROVIDER_PROD)
- `GET /meteo/avui` — today's vision summary
- `GET /meteo/historial` — last 48 analyses (unfiltered)
- `POST /meteo/analitza` — trigger immediate analysis
- `GET /meteo/validacio` — validation HTML page (sky classifications)
- `GET /meteo/foto/<date>/<file>` — serve photo by date/filename
- `GET /meteo/foto/abs/<path>` — serve photo by absolute path
- `GET /meteo/image` — latest.jpg
- `GET /health` — DB row counts

Dashboard endpoints (consumed by Android app):
- `GET /dashboard` — serves `dashboard_meteo.html`
- `GET /meteo/sensors/latest?station=X` — latest sensor reading
- `GET /meteo/sensors/avui?station=X` — min/max/mitja with timestamps
- `GET /meteo/sensors/historial24h?station=X` — 24h readings (for charts)
- `GET /meteo/fotos/dia?data=X&station=X&interval=N` — photos from sky_classifications
- `GET /meteo/fotos/directori?data=X&station=X&interval=N` — photos from filesystem (no BD)

## Key environment variables

`HA_TOKEN`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `ECOWITT_APP_KEY`, `ECOWITT_API_KEY`, `LOCAL_LLM_URL`, `METEO_PROVIDER`, `METEO_PROVIDER_PROD`, `METEO_STATION`.

## Commands

```bash
source /opt/meteo-analyst/venv/bin/activate

python3 /opt/meteo-analyst/meteo_analyst.py --force             # single vision analysis
python3 /opt/meteo-analyst/meteo_sky_classifier.py --data YYYYMMDD --station espui
python3 /opt/meteo-analyst/meteo_resum.py --data YYYY-MM-DD     # daily summary (skip if exists)
python3 /opt/meteo-analyst/meteo_resum.py --data YYYY-MM-DD --force  # force regenerate
python3 /opt/meteo-analyst/meteo_analisi_periode.py --dies 7    # N-day analysis
python3 /opt/meteo-analyst/meteo_ecowitt_history.py --station torrelles --start YYYY-MM-DD
python3 /opt/meteo-analyst/meteo_recalcula_sensors.py [--force] [--dry-run]
python3 /opt/meteo-analyst/meteo_collector.py --station torrelles --source ha
python3 /opt/meteo-analyst/meteo_collector.py --station espui --source ecowitt

# Interactive wrapper for sky classifier (prompts for confirmation)
./processa_dia.sh DD MM YYYY [--station espui] [--interval 15] [--provider gemini]

systemctl start|stop|restart meteo-api.service
```

## Dashboard (dashboard_meteo.html)

Single-page HTML+JS app served at `/dashboard`. Consumes the dashboard API endpoints. Features:
- **Station tabs**: Torrelles / Espui / Tot (comparative charts)
- **Cards**: Temp (incl. CH1 if present), Humitat, Pressió, Vent (with compass rose), Solar/UV, Pluja (incl. llamps condicional)
- **Min/max** with timestamps, arrows ↓↑, pale orange `#d4a574` for secondary values
- **Photo strip** (swipeable: touch, click sides, arrow keys, Escape)
- **Charts** (Chart.js): Temp+Humitat, Pressió, Vent+Ratxes (separate line), Pluja (conditional), Llamps (conditional), CH1 comparativa (conditional)
- **Static mode**: `?estacio=torrelles` hides tabs, always shows one station
- **Dynamic API prefix**: auto-detects local (`''`) vs remote (`/temps`) via DuckDNS

External access via Nginx Proxy Manager + DuckDNS → `/temps/dashboard`.

## Android app ("Datmet")

Minimal WebView wrapper (~50 lines Kotlin). Single activity that opens the dashboard URL. No UI logic beyond the WebView — the dashboard is the app. Exposed externally via NPM + DuckDNS.

## Conventions & quirks

- All logs go to both `/var/log/meteo-*.log` and stdout
- All timestamps are `Europe/Madrid` (TZ not explicit in code — assumes host TZ)
- Camera captures stored as `snapshot_YYYYMMDD_HHMMSS.jpg`; `latest.jpg` updated per capture
- **Photo path mismatch**: `capture.sh` writes to `/meteo/YYYYMMDD/` (no station), Python reads `/data/meteo/{station}/YYYYMMDD/` — assumes bind-mount/symlink bridge
- LLM analysis skips when sun elevation ≤ -5° (HA API); sky classifier skips outside 07:00–21:00
- `meteo_solar.py` used by API for accurate solar day detection (astral lib, per-station coordinates)
- `state.json` tracks capture counter for analysis rate-limiting (ANALYSE_EVERY = 1)
- All LLM prompts request JSON-only; parser strips ```json fences and JS comments
- `meteo_resum.py` skips if summary exists; use `--force` to regenerate
- `meteo_recalcula_sensors.py` imports from `meteo_sky_classifier`
- Collector has two data sources per station: HA API (torrelles) or Ecowitt cloud (espui)
- `sensors_ha` config in `meteo_collector.py:28-63` — each station can have different sensor entity IDs
- Espui station has NO HA integration yet (`ha_url: null`, `sensors_ha: {}`) — uses Ecowitt API directly
- Lightning (`lightning_distance`, `lightning_count`) and CH1 (`temp_ch1`, `hum_ch1`) fields in DB — may be `null` per station
- The CH1 card and chart appear in the dashboard **only when data exists** (conditional rendering)

## Gotchas

- Hardcoded paths everywhere (`/opt/meteo-analyst/`, `/data/meteo/`) — don't run scripts from git checkout
- `Claudemeteo_analyst.py` and `meteo_api.py.antic` are old/unused — ignore them
- `bak/` directory is gitignored — not part of active codebase
- `dashboard_meteo.html` at repo root, served by Flask from `/opt/meteo-analyst/`
- No tests, no linter, no typechecker configured
