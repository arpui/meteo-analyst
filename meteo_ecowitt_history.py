#!/usr/bin/env python3
# Versió: 2026-04-19 20:30
"""
Recuperació d'historial de sensors via API Ecowitt cloud
Insereix dades antigues a meteo_readings amb el camp station correcte

Ús:
  python3 meteo_ecowitt_history.py --station torrelles --start 2026-04-07
  python3 meteo_ecowitt_history.py --station espui --start 2026-01-01 --end 2026-04-19
  python3 meteo_ecowitt_history.py --station torrelles --start 2026-04-07 --dry-run
"""
import os
import math
import sqlite3
import logging
import argparse
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH  = Path("/data/meteo/meteo.db")
API_BASE = "https://api.ecowitt.net/api/v3"

# MACs dels dispositius Ecowitt
STATIONS = {
    "torrelles": "94:3C:C6:41:38:9F",   # GW2000A Güell Torrelles
    "espui":     "88:13:BF:46:4D:43",   # GW2000A Espui
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-ecowitt-history.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Conversions d'unitats ────────────────────────────────────────────────────

def f_to_c(f):
    """Fahrenheit → Celsius"""
    if f is None:
        return None
    return round((float(f) - 32) * 5 / 9, 1)

def mph_to_kmh(mph):
    """mph → km/h"""
    if mph is None:
        return None
    return round(float(mph) * 1.60934, 1)

def inhg_to_hpa(inhg):
    """inHg → hPa"""
    if inhg is None:
        return None
    return round(float(inhg) * 33.8639, 1)

def in_to_mm(inches):
    """inches → mm"""
    if inches is None:
        return None
    return round(float(inches) * 25.4, 1)

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def calcula_vpd(temp_c, humitat_pct):
    """Calcula Vapour Pressure Deficit (kPa) a partir de temp i humitat."""
    if temp_c is None or humitat_pct is None:
        return None
    try:
        es  = 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3))
        vpd = es * (1 - humitat_pct / 100)
        return round(vpd, 3)
    except Exception:
        return None

def rad_to_lux(rad_wm2):
    """Conversió aproximada W/m² → lux per llum solar (factor ~120)."""
    if rad_wm2 is None:
        return None
    return round(float(rad_wm2) * 120, 0)

# ─── API Ecowitt ──────────────────────────────────────────────────────────────

def fetch_period(mac: str, start: datetime, end: datetime) -> dict:
    """
    Recupera dades d'un període (màx recomanat: 1 dia per crida).
    Retorna dict amb les dades crues de l'API.
    """
    app_key = os.environ.get("ECOWITT_APP_KEY", "")
    api_key = os.environ.get("ECOWITT_API_KEY", "")

    if not app_key or not api_key:
        raise ValueError("ECOWITT_APP_KEY i ECOWITT_API_KEY han d'estar al .env")

    params = {
        "application_key": app_key,
        "api_key":         api_key,
        "mac":             mac,
        "start_date":      start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date":        end.strftime("%Y-%m-%d %H:%M:%S"),
        "cycle_type":      "5min",
        "call_back":       "outdoor,wind,pressure,rainfall,solar_and_uvi,indoor",
    }

    r = requests.get(f"{API_BASE}/device/history", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if data.get("code") != 0:
        raise ValueError(f"API error: {data.get('msg')} (code {data.get('code')})")

    return data.get("data", {})

def parse_readings(data: dict, station: str) -> list[dict]:
    """
    Converteix la resposta de l'API a llista de dicts per inserir a la BD.
    Cada entrada és un timestamp amb tots els sensors.
    """
    # Recull tots els timestamps disponibles
    if not isinstance(data, dict): return []
    timestamps = set()
    if not isinstance(data, dict): return []
    for grup in data.values():
        if isinstance(grup, dict) and "list" in grup:
            timestamps.update(grup["list"].keys())
        elif isinstance(grup, dict):
            for sensor in grup.values():
                if isinstance(sensor, dict) and "list" in sensor:
                    timestamps.update(sensor["list"].keys())

    readings = []
    for ts_unix in sorted(timestamps):
        dt = datetime.fromtimestamp(int(ts_unix))
        timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")

        def get_val(grup, sensor=None):
            try:
                if sensor:
                    return data[grup][sensor]["list"].get(ts_unix)
                return data[grup]["list"].get(ts_unix)
            except (KeyError, TypeError):
                return None

        # Temperatura exterior (°F → °C)
        temp_c    = f_to_c(get_val("outdoor", "temperature"))
        dew_c     = f_to_c(get_val("outdoor", "dew_point"))
        feel_c    = f_to_c(get_val("outdoor", "feels_like"))
        hum       = safe_float(get_val("outdoor", "humidity"))

        # Interior (°F → °C)
        temp_in_c = f_to_c(get_val("indoor", "temperature"))
        hum_in    = safe_float(get_val("indoor", "humidity"))

        # Pressió (inHg → hPa)
        pabs = inhg_to_hpa(get_val("pressure", "absolute"))
        prel = inhg_to_hpa(get_val("pressure", "relative"))

        # Vent (mph → km/h)
        wspd = mph_to_kmh(get_val("wind", "wind_speed"))
        wgst = mph_to_kmh(get_val("wind", "wind_gust"))
        wdir = safe_float(get_val("wind", "wind_direction"))

        # Solar i UV (W/m² ja correcte)
        rad = safe_float(get_val("solar_and_uvi", "solar"))
        uv  = safe_float(get_val("solar_and_uvi", "uvi"))

        # Pluja (inches → mm)
        rain_rate  = in_to_mm(get_val("rainfall", "rain_rate"))
        rain_daily = in_to_mm(get_val("rainfall", "daily"))

        readings.append({
            "timestamp":        timestamp,
            "station":          station,
            "temp_outdoor":     temp_c,
            "temp_feel":        feel_c,
            "temp_dewpoint":    dew_c,
            "temp_indoor":      temp_in_c,
            "temp_indoor_dew":  None,   # no sempre disponible
            "humidity":         int(hum) if hum is not None else None,
            "humidity_indoor":  int(hum_in) if hum_in is not None else None,
            "pressure_abs":     pabs,
            "pressure_rel":     prel,
            "vpd":              calcula_vpd(temp_c, hum),
            "wind_speed":       wspd,
            "wind_gust":        wgst,
            "wind_direction":   int(wdir) if wdir is not None else None,
            "wind_gust_max":    None,   # no disponible via history
            "solar_radiation":  rad,
            "solar_lux":        rad_to_lux(rad),
            "uv_index":         uv,
            "rain_rate":        rain_rate,
            "rain_hourly":      None,
            "rain_daily":       rain_daily,
            "rain_daily_piezo": None,
        })

    return readings

# ─── Base de dades ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    # Assegura columna station (migració si cal)
    try:
        conn.execute("ALTER TABLE meteo_readings ADD COLUMN station TEXT NOT NULL DEFAULT 'torrelles'")
        conn.commit()
        log.info("Migració BD: columna 'station' afegida")
    except Exception:
        pass
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_station_ts
            ON meteo_readings (station, timestamp)
        """)
        conn.commit()
    except Exception:
        pass
    conn.close()

def ja_existeix(conn, timestamp: str, station: str) -> bool:
    row = conn.execute(
        "SELECT id FROM meteo_readings WHERE timestamp = ? AND station = ?",
        (timestamp, station)
    ).fetchone()
    return row is not None

def insereix_readings(readings: list[dict], dry_run: bool = False) -> tuple[int, int]:
    """Insereix a la BD. Retorna (inserits, saltats)."""
    inserits = saltats = 0
    conn = sqlite3.connect(DB_PATH)

    for r in readings:
        if ja_existeix(conn, r["timestamp"], r["station"]):
            saltats += 1
            continue

        if not dry_run:
            conn.execute("""
                INSERT INTO meteo_readings (
                    timestamp, station,
                    temp_outdoor, temp_feel, temp_dewpoint, temp_indoor, temp_indoor_dew,
                    humidity, humidity_indoor,
                    pressure_abs, pressure_rel, vpd,
                    wind_speed, wind_gust, wind_direction, wind_gust_max,
                    solar_radiation, solar_lux, uv_index,
                    rain_rate, rain_hourly, rain_daily, rain_daily_piezo
                ) VALUES (
                    :timestamp, :station,
                    :temp_outdoor, :temp_feel, :temp_dewpoint, :temp_indoor, :temp_indoor_dew,
                    :humidity, :humidity_indoor,
                    :pressure_abs, :pressure_rel, :vpd,
                    :wind_speed, :wind_gust, :wind_direction, :wind_gust_max,
                    :solar_radiation, :solar_lux, :uv_index,
                    :rain_rate, :rain_hourly, :rain_daily, :rain_daily_piezo
                )
            """, r)
        inserits += 1

    if not dry_run:
        conn.commit()
    conn.close()
    return inserits, saltats

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Recuperació historial sensors via API Ecowitt cloud",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  %(prog)s --station torrelles --start 2026-04-07
  %(prog)s --station espui --start 2026-01-01 --end 2026-04-19
  %(prog)s --station torrelles --start 2026-04-07 --dry-run
        """
    )
    parser.add_argument("--station", required=True, choices=list(STATIONS.keys()),
                        help="Estació: torrelles | espui")
    parser.add_argument("--start",   required=True,
                        help="Data inici (YYYY-MM-DD)")
    parser.add_argument("--end",     default=None,
                        help="Data fi (YYYY-MM-DD), per defecte avui")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sense escriure a la BD")
    args = parser.parse_args()

    mac   = STATIONS[args.station]
    start = datetime.strptime(args.start, "%Y-%m-%d")
    end   = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now().replace(hour=23, minute=59, second=59)

    init_db()

    log.info(f"Recuperant historial [{args.station}] del {start.date()} al {end.date()}")
    if args.dry_run:
        log.info("MODE DRY-RUN — no s'escriurà res a la BD")

    total_inserits = total_saltats = total_errors = 0
    dia_actual = start

    while dia_actual <= end:
        dia_fi = min(dia_actual + timedelta(days=1) - timedelta(seconds=1), end)

        try:
            log.info(f"  Baixant {dia_actual.strftime('%Y-%m-%d')}...")
            data    = fetch_period(mac, dia_actual, dia_fi)
            readings = parse_readings(data, args.station)

            if not readings:
                log.warning(f"  Sense dades per {dia_actual.strftime('%Y-%m-%d')}")
            else:
                ins, sal = insereix_readings(readings, dry_run=args.dry_run)
                total_inserits += ins
                total_saltats  += sal
                log.info(f"  {dia_actual.strftime('%Y-%m-%d')}: {len(readings)} lectures → {ins} inserides, {sal} ja existien")

        except Exception as e:
            log.error(f"  Error {dia_actual.strftime('%Y-%m-%d')}: {e}")
            total_errors += 1

        dia_actual += timedelta(days=1)
        time.sleep(0.5)  # respecta rate limit API

    log.info(
        f"\nFet — Inserides: {total_inserits} | "
        f"Ja existien: {total_saltats} | "
        f"Errors: {total_errors}"
    )
    print(f"\nResultat: {total_inserits} inserides, {total_saltats} ja existien, {total_errors} errors")

if __name__ == "__main__":
    main()
