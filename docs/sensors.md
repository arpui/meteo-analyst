# Sensors

## Estació Torrelles

- **Font**: Home Assistant (`http://192.168.31.228:8123`)
- **MAC**: `94:3C:C6:41:38:9F`
- **Recol·lecció**: `meteo_collector.py --station torrelles --source ha`
- **Entitats HA**: 20+ sensors GW2000A (`sensor.gw2000a_v2_2_2_*`)

Torrelles té sensors **interiors i exteriors** complets. Dades de llamps i CH1 **no disponibles** via HA (sempre `null`).

## Estació Espui

- **Font**: Ecowitt cloud API (`https://api.ecowitt.net/api/v3`)
- **MAC**: `88:13:BF:46:4D:43`
- **Recol·lecció**: `meteo_collector.py --station espui --source ecowitt`
- **Sense HA**: `ha_url: null`, `sensors_ha: {}`

Espui obté les dades directament del núvol Ecowitt i **SI** inclou:
- **Llamps** (`lightning_distance`, `lightning_count`) — distància en km i comptador
- **CH1** (`temp_ch1`, `hum_ch1`) — sensor addicional (temperatura i humitat)

## Camps comuns

Totes les lectures (`meteo_readings`):

| Camp | Tipus | Descripció |
|------|-------|------------|
| `temp_outdoor` | REAL | Temperatura exterior (°C) |
| `temp_feel` | REAL | Sensació tèrmica (°C) |
| `temp_dewpoint` | REAL | Punt de rosada (°C) |
| `temp_indoor` | REAL | Temperatura interior (°C) |
| `humidity` | INTEGER | Humitat exterior (%) |
| `humidity_indoor` | INTEGER | Humitat interior (%) |
| `pressure_abs` | REAL | Pressió absoluta (hPa) |
| `pressure_rel` | REAL | Pressió relativa (hPa) |
| `wind_speed` | REAL | Velocitat vent (km/h) |
| `wind_gust` | REAL | Ratxa de vent (km/h) |
| `wind_direction` | INTEGER | Graus direcció vent (0-360) |
| `wind_gust_max` | REAL | Ratxa màxima diària (km/h) |
| `solar_radiation` | REAL | Radiació solar (W/m²) |
| `uv_index` | REAL | Índex UV |
| `rain_rate` | REAL | Pluja rate (mm/h) |
| `rain_hourly` | REAL | Pluja horària (mm) |
| `rain_daily` | REAL | Pluja diària (mm) |
| `lightning_distance` | REAL | Distància del llamp (km) — només Espui |
| `lightning_count` | INTEGER | Recompte de llamps — només Espui |
| `temp_ch1` | REAL | Temperatura sensor CH1 (°C) — només Espui |
| `hum_ch1` | INTEGER | Humitat sensor CH1 (%) — només Espui |

## Visualització condicional al dashboard

El dashboard mostra/amaga elements basant-se en si les dades existeixen per l'estació seleccionada:

- La **card CH1** i el **gràfic comparatiu CH1** apareixen NOMÉS si `temp_ch1 != null || hum_ch1 != null` en alguna lectura
- La **card de llamps** (dins de la card Pluja) apareix NOMÉS si `lightning_count != null || lightning_distance != null`
- El **gràfic de llamps** apareix NOMÉS si alguna lectura té `lightning_count > 0`
- El **gràfic de pluja** apareix NOMÉS si alguna lectura té `rain_rate > 0`

Això vol dir que per l'estació Torrelles (sense llamps ni CH1), aquests elements no es mostren mai al dashboard. Per Espui, si hi ha dades de CH1 i llamps, es mostren automàticament.
