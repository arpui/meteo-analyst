# API endpoints

Flask API al port 8765 (`meteo_api.py`). Dues categories: llegacy (integració HA) i dashboard (app Android).

## Legacy / HA

### `GET /meteo/latest`
Última anàlisi visual, filtrada per `METEO_PROVIDER_PROD`.

```json
{"timestamp":"2026-04-19 21:00:01","condició_general":"nocturn","cobertura_núvols":80,"precipitació":false,...}
```

### `GET /meteo/avui`
Resum del dia (mitja nuvolositat, hores de pluja, condició dominant).

### `GET /meteo/historial`
Últimes 48 anàlisis (sense filtrar per provider).

### `POST /meteo/analitza`
Força anàlisi immediata de `latest.jpg`. Executa `meteo_analyst.py --force` via subprocess.

### `GET /meteo/image`
Retorna `latest.jpg` de l'estació activa.

### `GET /meteo/combined/latest`
Sensors + anàlisi visual combinats (últim de cada taula).

### `GET /meteo/combined/avui`
Resum del dia combinant sensors i visió.

### `GET /health`
Comptes de files a cada taula: `readings`, `analisis`, `sky_classifications`.

## Dashboard

### `GET /dashboard`
Serveix `dashboard_meteo.html`.

### `GET /meteo/sensors/latest?station=torrelles|espui`
Última lectura de sensors per estació. Retorna TOTES les columnes de `meteo_readings`:
`temp_outdoor`, `temp_feel`, `temp_dewpoint`, `humidity`, `pressure_rel`, `wind_speed`, `wind_gust`, `wind_direction`, `solar_radiation`, `uv_index`, `rain_rate`, `rain_daily`, `lightning_distance`, `lightning_count`, `temp_ch1`, `hum_ch1`, etc.

### `GET /meteo/sensors/avui?station=torrelles|espui`
Min/max/mitja del dia amb timestamps:
```json
{
  "temp_min": 12.3, "temp_min_ts": "2026-04-19 06:15:00",
  "temp_max": 21.8, "temp_max_ts": "2026-04-19 14:30:00",
  "gust_max": 25.4, "gust_max_ts": "...",
  "lightning_events": 3, "lightning_max": 2,
  ...
}
```

### `GET /meteo/sensors/historial24h?station=torrelles|espui`
Totes les lectures de les últimes 24h per gràfics. Camps seleccionats (inclou lightning i CH1 si existeixen).

### `GET /meteo/fotos/dia?data=YYYYMMDD&station=X&interval=30`
Fotos classificades (`sky_classifications`) d'un dia, filtrant diürnes, una cada `interval` minuts.

### `GET /meteo/fotos/directori?data=YYYYMMDD&station=X&interval=30`
Fotos llegides directament del filesystem (no requereix `sky_classifications`). Retorna `fitxer` (path absolut) + `timestamp`.

### `GET /meteo/foto/abs/<path>`
Serveix una foto pel seu path absolut: `/meteo/foto/abs/data/meteo/espui/20251227/snapshot_120000.jpg`.

### `GET /meteo/foto/<date>/<file>`
Serveix foto per data i nom: `/meteo/foto/20260414/snapshot_070001.jpg`.

### `GET /meteo/validacio`
Pàgina HTML per validar classificacions del cel. Query params:
- `data=YYYYMMDD` — filtra per dia
- `limit=N` — nombre de resultats (default 20)
- `nocturnes=1` — inclou nocturnes
- `tot=1` — inclou qualitat dolenta
- `ordre=comparar` — mostra mateixa foto classificada per diferents providers/models
