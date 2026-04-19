#!/usr/bin/env python3
"""
Migració i recàlcul de sensors per sky_classifications:
  1. Afegeix columna 'station' a sky_classifications (default 'torrelles')
  2. Afegeix columna 'station' a sky_classifications si no existeix
  3. Recalcula delta_pressio_1h, rain_rate_moment, temp_moment,
     confianca_coherencia i confianca_total per classificacions
     que no tenen dades de sensor (o totes si --force)

Ús:
  python3 meteo_recalcula_sensors.py           # només les que falten
  python3 meteo_recalcula_sensors.py --force   # totes
  python3 meteo_recalcula_sensors.py --dry-run # simula sense escriure
"""
import sqlite3
import logging
import argparse
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

import sys
sys.path.insert(0, "/opt/meteo-analyst")
from meteo_sky_classifier import sensors_del_moment, calcula_coherencia

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH = Path("/data/meteo/meteo.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-recalcula-sensors.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Migració BD ─────────────────────────────────────────────────────────────

def migra_bd():
    conn = sqlite3.connect(DB_PATH)

    # Afegeix station a sky_classifications
    try:
        conn.execute("ALTER TABLE sky_classifications ADD COLUMN station TEXT NOT NULL DEFAULT 'torrelles'")
        conn.commit()
        log.info("Migració: columna 'station' afegida a sky_classifications")
    except Exception:
        log.info("Columna 'station' ja existeix a sky_classifications")

    # Actualitza les files existents a torrelles (per si el DEFAULT no s'ha aplicat)
    conn.execute("UPDATE sky_classifications SET station = 'torrelles' WHERE station IS NULL OR station = ''")
    conn.commit()

    # Índex
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sky_station_ts
            ON sky_classifications (station, timestamp)
        """)
        conn.commit()
    except Exception:
        pass

    conn.close()

# ─── Recàlcul ────────────────────────────────────────────────────────────────

def confianca_total(llm, coh):
    valors = [v for v in [llm, coh] if v is not None]
    if not valors:
        return None
    return round(sum(valors) / len(valors))

def recalcula(force: bool = False, dry_run: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if force:
        rows = conn.execute(
            "SELECT * FROM sky_classifications ORDER BY timestamp ASC"
        ).fetchall()
        log.info(f"Mode --force: recalculant totes les {len(rows)} classificacions")
    else:
        rows = conn.execute("""
            SELECT * FROM sky_classifications
            WHERE temp_moment IS NULL
               OR delta_pressio_1h IS NULL
            ORDER BY timestamp ASC
        """).fetchall()
        log.info(f"Classificacions sense sensors: {len(rows)}")

    if not rows:
        log.info("Res a recalcular")
        conn.close()
        return

    ok = saltats = errors = 0

    for row in rows:
        timestamp = row["timestamp"]
        try:
            # Recupera dades LLM del raw_json per recalcular coherència
            raw = json.loads(row["raw_json"]) if row["raw_json"] else {}
            raw["_hora_context"] = datetime.strptime(
                timestamp, "%Y-%m-%d %H:%M:%S"
            ).strftime("%H:%M")

            sensors = sensors_del_moment(timestamp)
            if not sensors:
                saltats += 1
                continue

            coh   = calcula_coherencia(raw, sensors)
            conf  = confianca_total(row["confianca_llm"], coh)

            if not dry_run:
                conn.execute("""
                    UPDATE sky_classifications SET
                        delta_pressio_1h     = ?,
                        rain_rate_moment     = ?,
                        temp_moment          = ?,
                        confianca_coherencia = ?,
                        confianca_total      = ?
                    WHERE id = ?
                """, (
                    sensors.get("delta_pressio_1h"),
                    sensors.get("rain_rate_moment"),
                    sensors.get("temp_moment"),
                    coh,
                    conf,
                    row["id"]
                ))

            log.info(
                f"OK {row['fitxer'].split('/')[-1]} | "
                f"T:{sensors.get('temp_moment')}°C | "
                f"Δp:{sensors.get('delta_pressio_1h')} | "
                f"coh:{coh} | conf:{conf}"
            )
            ok += 1

        except Exception as e:
            log.error(f"Error {timestamp}: {e}")
            errors += 1

    if not dry_run:
        conn.commit()
    conn.close()

    log.info(f"Fet — OK:{ok} | Sense sensors:{saltats} | Errors:{errors}")
    print(f"\nResultat: {ok} actualitzades, {saltats} sense sensors, {errors} errors")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Migració station + recàlcul sensors sky_classifications"
    )
    parser.add_argument("--force",   action="store_true",
                        help="Recalcula totes, no només les que falten")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sense escriure a la BD")
    args = parser.parse_args()

    if args.dry_run:
        log.info("MODE DRY-RUN — no s'escriurà res a la BD")

    migra_bd()
    recalcula(force=args.force, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
