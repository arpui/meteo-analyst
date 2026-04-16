#!/usr/bin/env python3
"""
Col·lector de sensors meteorològics Ecowitt via Home Assistant REST API
Cron cada 5min al LXC meteo → desa a /data/meteo/meteo.db (mountbind persistent)
"""
from dotenv import load_dotenv
load_dotenv("/opt/meteo-analyst/.env")

import os
import sqlite3
import logging
import requests
from datetime import datetime
from pathlib import Path

# ─── Configuració ────────────────────────────────────────────────────────────

HA_URL   = "http://192.168.31.228:8123"
HA_TOKEN = os.environ.get("HA_TOKEN", "")
DB_PATH  = Path("/data/meteo/meteo.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-collector.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Mapa de sensors ─────────────────────────────────────────────────────────

SENSORS = {
    # Temperatura
    "temp_outdoor":     "sensor.gw2000a_v2_2_2_outdoor_temperature",
    "temp_feel":        "sensor.gw2000a_v2_2_2_feels_like_temperature",
    "temp_dewpoint":    "sensor.gw2000a_v2_2_2_dewpoint",
    "temp_indoor":      "sensor.gw2000a_v2_2_2_indoor_temperature",
    "temp_indoor_dew":  "sensor.gw2000a_v2_2_2_indoor_dewpoint",
    # Humitat
    "humidity":         "sensor.gw2000a_v2_2_2_humidity",
    "humidity_indoor":  "sensor.gw2000a_v2_2_2_indoor_humidity",
    # Pressió
    "pressure_abs":     "sensor.gw2000a_v2_2_2_absolute_pressure",
    "pressure_rel":     "sensor.gw2000a_v2_2_2_relative_pressure",
    "vpd":              "sensor.gw2000a_vapour_pressure_deficit",
    # Vent
    "wind_speed":       "sensor.gw2000a_v2_2_2_wind_speed",
    "wind_gust":        "sensor.gw2000a_v2_2_2_wind_gust",
    "wind_direction":   "sensor.gw2000a_v2_2_2_wind_direction",
    "wind_gust_max":    "sensor.gw2000a_v2_2_2_max_daily_gust",
    # Solar
    "solar_radiation":  "sensor.gw2000a_v2_2_2_solar_radiation",
    "solar_lux":        "sensor.gw2000a_v2_2_2_solar_lux",
    "uv_index":         "sensor.gw2000a_v2_2_2_uv_index",
    # Pluja
    "rain_rate":        "sensor.gw2000a_rain_rate",
    "rain_hourly":      "sensor.gw2000a_hourly_rain_rate",
    "rain_daily":       "sensor.gw2000a_daily_rain_rate",
    "rain_daily_piezo": "sensor.gw2000a_v2_2_2_daily_rain_rate_piezo",
}

# ─── Base de dades ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meteo_readings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            -- Temperatura
            temp_outdoor     REAL,
            temp_feel        REAL,
            temp_dewpoint    REAL,
            temp_indoor      REAL,
            temp_indoor_dew  REAL,
            -- Humitat
            humidity         INTEGER,
            humidity_indoor  INTEGER,
            -- Pressió
            pressure_abs     REAL,
            pressure_rel     REAL,
            vpd              REAL,
            -- Vent
            wind_speed       REAL,
            wind_gust        REAL,
            wind_direction   INTEGER,
            wind_gust_max    REAL,
            -- Solar
            solar_radiation  REAL,
            solar_lux        REAL,
            uv_index         REAL,
            -- Pluja
            rain_rate        REAL,
            rain_hourly      REAL,
            rain_daily       REAL,
            rain_daily_piezo REAL
        )
    """)
    conn.commit()
    conn.close()

def desa_lectura(dades: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO meteo_readings (
            timestamp,
            temp_outdoor, temp_feel, temp_dewpoint, temp_indoor, temp_indoor_dew,
            humidity, humidity_indoor,
            pressure_abs, pressure_rel, vpd,
            wind_speed, wind_gust, wind_direction, wind_gust_max,
            solar_radiation, solar_lux, uv_index,
            rain_rate, rain_hourly, rain_daily, rain_daily_piezo
        ) VALUES (
            :timestamp,
            :temp_outdoor, :temp_feel, :temp_dewpoint, :temp_indoor, :temp_indoor_dew,
            :humidity, :humidity_indoor,
            :pressure_abs, :pressure_rel, :vpd,
            :wind_speed, :wind_gust, :wind_direction, :wind_gust_max,
            :solar_radiation, :solar_lux, :uv_index,
            :rain_rate, :rain_hourly, :rain_daily, :rain_daily_piezo
        )
    """, dades)
    conn.commit()
    conn.close()

# ─── Crida a HA ──────────────────────────────────────────────────────────────

def get_sensor(session: requests.Session, entity_id: str) -> float | None:
    try:
        r = session.get(
            f"{HA_URL}/api/states/{entity_id}",
            timeout=5
        )
        r.raise_for_status()
        estat = r.json().get("state")
        if estat in (None, "unavailable", "unknown"):
            return None
        return float(estat)
    except Exception as e:
        log.warning(f"Error llegint {entity_id}: {e}")
        return None

def recull_sensors() -> dict:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    })

    dades = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    for camp, entity_id in SENSORS.items():
        dades[camp] = get_sensor(session, entity_id)
        log.debug(f"  {camp}: {dades[camp]}")

    return dades

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not HA_TOKEN:
        log.error("HA_TOKEN no definit — exporta la variable d'entorn")
        raise SystemExit(1)

    init_db()

    log.info("Recollint sensors Ecowitt...")
    dades = recull_sensors()

    desa_lectura(dades)
    log.info(
        f"OK — {dades['timestamp']} | "
        f"T: {dades['temp_outdoor']}°C | "
        f"H: {dades['humidity']}% | "
        f"Vent: {dades['wind_speed']} km/h | "
        f"Pluja: {dades['rain_rate']} mm/h"
    )

if __name__ == "__main__":
    main()