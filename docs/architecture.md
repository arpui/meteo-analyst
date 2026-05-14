# Arquitectura del sistema

## Visió general

Sistema de monitorització meteorològica amb dues estacions Ecowitt, càmera IP, anàlisi d'imatges per LLM, i dashboard en temps real consumit per una app Android.

## Components

```
Proxmox host
├── càmera HP10 (192.168.31.182) → capture.sh → /meteo/YYYYMMDD/snapshot_*.jpg
└── LXC meteo (container)
    ├── meteo_collector.py   ─→ polls sensors (HA API o Ecowitt cloud) cada 5min
    ├── meteo_analyst.py     ─→ analitza latest.jpg amb LLM vision
    ├── meteo_api.py         ─→ Flask API + dashboard (port 8765)
    ├── meteo_sky_classifier.py → classificació detallada del cel (manual)
    ├── meteo_resum.py       ─→ resum narratiu diari (cron)
    ├── meteo_ecowitt_history.py → importa històric Ecowitt cloud
    └── meteo_recalcula_sensors.py → backfill sensors a sky_classifications

Home Assistant (192.168.31.228:8123)
    └── rep analisis de meteo_analyst.py (estació torrelles)

Externament
    ├── Nginx Proxy Manager → DuckDNS → /temps/dashboard
    └── App Android (Datmet) → WebView → URL DuckDNS
```

## Flux de dades

### Sensors
1. Torrelles: `meteo_collector.py --source ha` → HA API (192.168.31.228:8123) → SQLite
2. Espui: `meteo_collector.py --source ecowitt` → Ecowitt cloud API → SQLite

Les dues s'executen cada 5 minuts via cron.

### Fotos
1. `capture.sh` (al Proxmox host) captura de la càmera cada 2-5 min
2. Desa a `/meteo/YYYYMMDD/snapshot_HHMMSS.jpg` i actualitza `latest.jpg`
3. El directori `/meteo/` del host es monta com a `/data/meteo/` al LXC

### Anàlisi visual
1. `meteo_analyst.py` detecta captures noves via `state.json` (contador)
2. Quan toca, crida al LLM (Claude/OpenAI/Gemini) amb `latest.jpg`
3. Desa el resultat a la taula `analisis`
4. Notifica HA via webhook

### Dashboard + Android
1. L'API Flask serveix `dashboard_meteo.html` a `/dashboard`
2. L'app Android obre la URL del DuckDNS en una WebView
3. El dashboard fa fetch dels endpoints `/meteo/sensors/*` i `/meteo/fotos/*`
4. Refresca cada 60 segons

## Unitats i conversions

Totes les conversions es fan al collector:
- Temperatura: °F → °C
- Pressió: inHg → hPa
- Vent: mph → km/h
- Pluja: inches → mm
- Radiació solar → lux (×120)
- Llamp (Ecowitt): milles → km

## Xarxa

| Host | IP | Rol |
|------|-----|-----|
| Càmera HP10 | 192.168.31.182 | Captura fotos |
| Home Assistant | 192.168.31.228:8123 | Font sensors Torrelles |
| LXC Meteo | 192.168.31.225:8765 | API + dashboard |
| DuckDNS | xxx.duckdns.org | Accés extern via NPM |

## Android app (Datmet)

No hi ha codi font al repositori. És una app Kotlin minimalista (~50 línies) que:
1. Obre una WebView apuntant a la URL del dashboard
2. Sense UI addicional — el dashboard és l'app
3. Accés extern via Nginx Proxy Manager → `/temps/dashboard`
