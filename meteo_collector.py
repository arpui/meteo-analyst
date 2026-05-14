#!/usr/bin/env python3
# Versió: 2026-04-24 14:45
"""
Col·lector de sensors meteorològics — multi-estació, multi-font
Fonts suportades: ha (Home Assistant), ecowitt (API cloud)
Ús:
  python3 meteo_collector.py --station torrelles --source ha
  python3 meteo_collector.py --station espui --source ecowitt
Cron:
  */5 * * * * /opt/meteo-analyst/venv/bin/python3 /opt/meteo-analyst/meteo_collector.py --station torrelles --source ha
  */5 * * * * /opt/meteo-analyst/venv/bin/python3 /opt/meteo-analyst/meteo_collector.py --station espui --source ecowitt
"""
from dotenv import load_dotenv
load_dotenv("/opt/meteo-analyst/.env")

import os
import sys
import math
import sqlite3
import logging
import argparse
import requests
from datetime import datetime
from pathlib import Path

# ─── Configuració d'estacions ────────────────────────────────────────────────

ESTACIONS = {
    "torrelles": {
        "source":  "ha",                      # font per defecte
        "mac":     "94:3C:C6:41:38:9F",       # GW2000A Torrelles
        "ha_url":  "http://192.168.31.228:8123",
        "sensors_ha": {
            "temp_outdoor":     "sensor.gw2000a_v2_2_2_outdoor_temperature",
            "temp_feel":        "sensor.gw2000a_v2_2_2_feels_like_temperature",
            "temp_dewpoint":    "sensor.gw2000a_v2_2_2_dewpoint",
            "temp_indoor":      "sensor.gw2000a_v2_2_2_indoor_temperature",
            "temp_indoor_dew":  "sensor.gw2000a_v2_2_2_indoor_dewpoint",
            "humidity":         "sensor.gw2000a_v2_2_2_humidity",
            "humidity_indoor":  "sensor.gw2000a_v2_2_2_indoor_humidity",
            "pressure_abs":     "sensor.gw2000a_v2_2_2_absolute_pressure",
            "pressure_rel":     "sensor.gw2000a_v2_2_2_relative_pressure",
            "vpd":              "sensor.gw2000a_vapour_pressure_deficit",
            "wind_speed":       "sensor.gw2000a_v2_2_2_wind_speed",
            "wind_gust":        "sensor.gw2000a_v2_2_2_wind_gust",
            "wind_direction":   "sensor.gw2000a_v2_2_2_wind_direction",
            "wind_gust_max":    "sensor.gw2000a_v2_2_2_max_daily_gust",
            "solar_radiation":  "sensor.gw2000a_v2_2_2_solar_radiation",
            "solar_lux":        "sensor.gw2000a_v2_2_2_solar_lux",
            "uv_index":         "sensor.gw2000a_v2_2_2_uv_index",
            "rain_rate":        "sensor.gw2000a_rain_rate",
            "rain_hourly":      "sensor.gw2000a_hourly_rain_rate",
            "rain_daily":       "sensor.gw2000a_daily_rain_rate",
            "rain_daily_piezo": "sensor.gw2000a_v2_2_2_daily_rain_rate_piezo",
        },
    },
    "espui": {
        "source":  "ecowitt",                 # font per defecte
        "mac":     "88:13:BF:46:4D:43",       # GW2000A Espui
        "ha_url":  None,                      # futur: IP ZeroTier
        "sensors_ha": {},                     # futur: entitats HA Espui
    },
}

DB_PATH    = Path("/data/meteo/meteo.db")
ECOWITT_BASE = "https://api.ecowitt.net/api/v3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-collector.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Conversions ─────────────────────────────────────────────────────────────

def safe_float(v):
    if v is None or v == '-' or v == '': return None
    try: return float(v)
    except: return None

def f_to_c(v):
    x = safe_float(v)
    return round((x - 32) * 5 / 9, 1) if x is not None else None

def mph_to_kmh(v):
    x = safe_float(v)
    return round(x * 1.60934, 1) if x is not None else None

def inhg_to_hpa(v):
    x = safe_float(v)
    return round(x * 33.8639, 1) if x is not None else None

def in_to_mm(v):
    x = safe_float(v)
    return round(x * 25.4, 1) if x is not None else None

def mi_to_km(v):
    x = safe_float(v)
    return round(x * 1.60934, 1) if x is not None else None

def calcula_vpd(temp_c, hum):
    if temp_c is None or hum is None: return None
    try:
        es = 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3))
        return round(es * (1 - hum / 100), 3)
    except: return None

def rad_to_lux(v):
    x = safe_float(v)
    return round(x * 120, 0) if x is not None else None

# ─── Base de dades ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meteo_readings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            station          TEXT NOT NULL DEFAULT 'torrelles',
            temp_outdoor     REAL, temp_feel       REAL, temp_dewpoint   REAL,
            temp_indoor      REAL, temp_indoor_dew REAL,
            humidity         INTEGER, humidity_indoor INTEGER,
            pressure_abs     REAL, pressure_rel    REAL, vpd             REAL,
            wind_speed       REAL, wind_gust       REAL, wind_direction  INTEGER,
            wind_gust_max    REAL,
            solar_radiation  REAL, solar_lux       REAL, uv_index        REAL,
            rain_rate        REAL, rain_hourly     REAL, rain_daily      REAL,
            rain_daily_piezo REAL,
            lightning_distance REAL, lightning_count INTEGER,
            temp_ch1 REAL, hum_ch1 INTEGER
        )
    """)
    # Migracions no destructives
    for col, tipus in [("lightning_distance", "REAL"), ("lightning_count", "INTEGER"), ("temp_ch1", "REAL"), ("hum_ch1", "INTEGER")]:
        try:
            conn.execute(f"ALTER TABLE meteo_readings ADD COLUMN {col} {tipus}")
            conn.commit()
        except: pass
    for col, defval in [("station", "'torrelles'")]:
        try:
            conn.execute(f"ALTER TABLE meteo_readings ADD COLUMN {col} TEXT NOT NULL DEFAULT {defval}")
            conn.commit()
        except: pass
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_station_ts
            ON meteo_readings (station, timestamp)
        """)
        conn.commit()
    except: pass
    conn.commit()
    conn.close()

def desa_lectura(dades: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO meteo_readings (
            timestamp, station,
            temp_outdoor, temp_feel, temp_dewpoint, temp_indoor, temp_indoor_dew,
            humidity, humidity_indoor,
            pressure_abs, pressure_rel, vpd,
            wind_speed, wind_gust, wind_direction, wind_gust_max,
            solar_radiation, solar_lux, uv_index,
            rain_rate, rain_hourly, rain_daily, rain_daily_piezo,
            lightning_distance, lightning_count,
            temp_ch1, hum_ch1
        ) VALUES (
            :timestamp, :station,
            :temp_outdoor, :temp_feel, :temp_dewpoint, :temp_indoor, :temp_indoor_dew,
            :humidity, :humidity_indoor,
            :pressure_abs, :pressure_rel, :vpd,
            :wind_speed, :wind_gust, :wind_direction, :wind_gust_max,
            :solar_radiation, :solar_lux, :uv_index,
            :rain_rate, :rain_hourly, :rain_daily, :rain_daily_piezo,
            :lightning_distance, :lightning_count,
            :temp_ch1, :hum_ch1
        )
    """, dades)
    conn.commit()
    conn.close()

# ─── Font: Home Assistant ─────────────────────────────────────────────────────

def recull_ha(station: str, cfg: dict) -> dict:
    ha_url   = cfg["ha_url"]
    ha_token = os.environ.get("HA_TOKEN", "")
    if not ha_token:
        raise ValueError("HA_TOKEN no definit")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {ha_token}",
        "Content-Type":  "application/json",
    })

    def get(entity_id):
        try:
            r = session.get(f"{ha_url}/api/states/{entity_id}", timeout=5)
            r.raise_for_status()
            estat = r.json().get("state")
            if estat in (None, "unavailable", "unknown"):
                return None
            return float(estat)
        except Exception as e:
            log.warning(f"HA error {entity_id}: {e}")
            return None

    s = cfg["sensors_ha"]
    return {
        "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "station":          station,
        "temp_outdoor":     get(s.get("temp_outdoor","")),
        "temp_feel":        get(s.get("temp_feel","")),
        "temp_dewpoint":    get(s.get("temp_dewpoint","")),
        "temp_indoor":      get(s.get("temp_indoor","")),
        "temp_indoor_dew":  get(s.get("temp_indoor_dew","")),
        "humidity":         get(s.get("humidity","")),
        "humidity_indoor":  get(s.get("humidity_indoor","")),
        "pressure_abs":     get(s.get("pressure_abs","")),
        "pressure_rel":     get(s.get("pressure_rel","")),
        "vpd":              get(s.get("vpd","")),
        "wind_speed":       get(s.get("wind_speed","")),
        "wind_gust":        get(s.get("wind_gust","")),
        "wind_direction":   get(s.get("wind_direction","")),
        "wind_gust_max":    get(s.get("wind_gust_max","")),
        "solar_radiation":  get(s.get("solar_radiation","")),
        "solar_lux":        get(s.get("solar_lux","")),
        "uv_index":         get(s.get("uv_index","")),
        "rain_rate":        get(s.get("rain_rate","")),
        "rain_hourly":      get(s.get("rain_hourly","")),
        "rain_daily":       get(s.get("rain_daily","")),
        "rain_daily_piezo": get(s.get("rain_daily_piezo","")),
        "lightning_distance": None,
        "lightning_count":    None,
        "temp_ch1":           None,
        "hum_ch1":            None,
    }

# ─── Font: Ecowitt API real_time ──────────────────────────────────────────────

def recull_ecowitt(station: str, cfg: dict) -> dict:
    app_key = os.environ.get("ECOWITT_APP_KEY", "")
    api_key = os.environ.get("ECOWITT_API_KEY", "")
    if not app_key or not api_key:
        raise ValueError("ECOWITT_APP_KEY / ECOWITT_API_KEY no definits")

    r = requests.get(f"{ECOWITT_BASE}/device/real_time", params={
        "application_key": app_key,
        "api_key":         api_key,
        "mac":             cfg["mac"],
        "call_back":       "outdoor,indoor,wind,pressure,rainfall,solar_and_uvi,lightning,temp_and_humidity_ch1",
    }, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise ValueError(f"Ecowitt error: {data.get('msg')}")

    d = data.get("data", {})

    def val(grup, camp):
        try: return d[grup][camp]["value"]
        except: return None

    temp_c = f_to_c(val("outdoor", "temperature"))
    hum    = safe_float(val("outdoor", "humidity"))

    return {
        "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "station":          station,
        "temp_outdoor":     temp_c,
        "temp_feel":        f_to_c(val("outdoor", "feels_like")),
        "temp_dewpoint":    f_to_c(val("outdoor", "dew_point")),
        "temp_indoor":      f_to_c(val("indoor", "temperature")),
        "temp_indoor_dew":  None,
        "humidity":         int(hum) if hum is not None else None,
        "humidity_indoor":  int(safe_float(val("indoor", "humidity")) or 0) or None,
        "pressure_abs":     inhg_to_hpa(val("pressure", "absolute")),
        "pressure_rel":     inhg_to_hpa(val("pressure", "relative")),
        "vpd":              calcula_vpd(temp_c, hum),
        "wind_speed":       mph_to_kmh(val("wind", "wind_speed")),
        "wind_gust":        mph_to_kmh(val("wind", "wind_gust")),
        "wind_direction":   int(safe_float(val("wind", "wind_direction")) or 0) or None,
        "wind_gust_max":    mph_to_kmh(val("wind", "wind_gust_daily_max")),
        "solar_radiation":  safe_float(val("solar_and_uvi", "solar")),
        "solar_lux":        rad_to_lux(val("solar_and_uvi", "solar")),
        "uv_index":         safe_float(val("solar_and_uvi", "uvi")),
        "rain_rate":        in_to_mm(val("rainfall", "rain_rate")),
        "rain_hourly":      in_to_mm(val("rainfall", "hourly")),
        "rain_daily":       in_to_mm(val("rainfall", "daily")),
        "rain_daily_piezo": None,
        "lightning_distance": mi_to_km(val("lightning", "distance")),
        "lightning_count":    int(safe_float(val("lightning", "count")) or 0) if val("lightning", "count") is not None else None,
        "temp_ch1":           f_to_c(val("temp_and_humidity_ch1", "temperature")),
        "hum_ch1":            int(safe_float(val("temp_and_humidity_ch1", "humidity")) or 0) or None,
    }

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Col·lector de sensors meteorològics multi-estació"
    )
    parser.add_argument("--station", default=os.environ.get("METEO_STATION", "torrelles"),
                        choices=list(ESTACIONS.keys()),
                        help="Estació: torrelles | espui")
    parser.add_argument("--source",  default=None,
                        choices=["ha", "ecowitt"],
                        help="Font de dades (per defecte: la configurada per l'estació)")
    args = parser.parse_args()

    cfg    = ESTACIONS[args.station]
    source = args.source or cfg["source"]

    init_db()
    log.info(f"Recollint sensors [{args.station}] via {source}...")

    try:
        if source == "ha":
            dades = recull_ha(args.station, cfg)
        elif source == "ecowitt":
            dades = recull_ecowitt(args.station, cfg)
        else:
            raise ValueError(f"Font desconeguda: {source}")

        desa_lectura(dades)
        log.info(
            f"OK [{args.station}/{source}] — {dades['timestamp']} | "
            f"T: {dades['temp_outdoor']}°C | "
            f"H: {dades['humidity']}% | "
            f"Vent: {dades['wind_speed']} km/h | "
            f"Pluja: {dades['rain_rate']} mm/h"
        )
    except Exception as e:
        log.error(f"Error [{args.station}/{source}]: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
