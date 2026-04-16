#!/usr/bin/env python3
"""
Resum diari meteorològic amb Claude
Agafa dades del dia de meteo_readings + analisis i genera un text narratiu
Desa el resum a la BD i opcionalment notifica HA
"""
import os
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import anthropic
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH  = Path("/data/meteo/meteo.db")
HA_URL   = "http://192.168.31.228:8123"
HA_TOKEN = os.environ.get("HA_TOKEN", "")
MODEL    = "claude-haiku-4-5-20251001"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-resum.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Base de dades ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resums (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            data      TEXT NOT NULL,
            resum     TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def desa_resum(data: str, resum: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO resums (timestamp, data, resum)
        VALUES (?, ?, ?)
    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data, resum))
    conn.commit()
    conn.close()

def resum_ja_existeix(data: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT id FROM resums WHERE data = ?", (data,)
    ).fetchone()
    conn.close()
    return row is not None

# ─── Recull dades del dia ─────────────────────────────────────────────────────

def recull_dades_dia(data: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    readings = conn.execute("""
        SELECT * FROM meteo_readings
        WHERE timestamp LIKE ?
        ORDER BY id ASC
    """, (f"{data}%",)).fetchall()

    analisis = conn.execute("""
        SELECT * FROM analisis
        WHERE timestamp LIKE ?
        ORDER BY id ASC
    """, (f"{data}%",)).fetchall()

    conn.close()
    return {"readings": readings, "analisis": analisis}

def prepara_context(data: str, dades: dict) -> str:
    readings = dades["readings"]
    analisis = dades["analisis"]

    lines = [f"Data: {data}"]

    if readings:
        temps  = [r["temp_outdoor"] for r in readings if r["temp_outdoor"] is not None]
        humits = [r["humidity"]     for r in readings if r["humidity"]     is not None]
        vents  = [r["wind_speed"]   for r in readings if r["wind_speed"]   is not None]
        ratxes = [r["wind_gust"]    for r in readings if r["wind_gust"]    is not None]
        pluja  = readings[-1]["rain_daily"] if readings else 0
        uv     = [r["uv_index"]     for r in readings if r["uv_index"]     is not None]
        rad    = [r["solar_radiation"] for r in readings if r["solar_radiation"] is not None]

        lines.append(f"\n— SENSORS ECOWITT ({len(readings)} lectures) —")
        if temps:
            lines.append(f"Temperatura exterior: min {min(temps):.1f}°C, max {max(temps):.1f}°C, mitja {sum(temps)/len(temps):.1f}°C")
        if humits:
            lines.append(f"Humitat: min {min(humits):.0f}%, max {max(humits):.0f}%, mitja {sum(humits)/len(humits):.0f}%")
        if vents:
            lines.append(f"Vent: mitja {sum(vents)/len(vents):.1f} km/h, ratxa màx {max(ratxes):.1f} km/h")
        if pluja:
            lines.append(f"Pluja acumulada dia: {pluja:.1f} mm")
        if uv:
            lines.append(f"UV màxim: {max(uv):.1f}")
        if rad:
            lines.append(f"Radiació solar màxima: {max(rad):.1f} W/m²")

    if analisis:
        condicions = [r["condició_general"] for r in analisis if r["condició_general"]]
        condicio_freq = max(set(condicions), key=condicions.count) if condicions else None
        mitja_nuvolos = sum(r["cobertura_núvols"] or 0 for r in analisis) / len(analisis)
        hores_pluja   = sum(1 for r in analisis if r["precipitació"]) * 0.5
        obs = [r["observacions"] for r in analisis if r["observacions"]]

        lines.append(f"\n— ANÀLISI VISUAL ({len(analisis)} anàlisis) —")
        lines.append(f"Condició dominant: {condicio_freq}")
        lines.append(f"Nuvolositat mitjana: {mitja_nuvolos:.0f}%")
        if hores_pluja:
            lines.append(f"Hores amb precipitació: {hores_pluja:.1f}h")
        if obs:
            lines.append(f"Observacions destacades: {'; '.join(obs[-3:])}")

    return "\n".join(lines)

# ─── Crida a Claude ───────────────────────────────────────────────────────────

PROMPT_SISTEMA = """Ets un meteoròleg local que escriu resums del temps per a una estació meteorològica domèstica. 
Escriu en català, de forma clara i natural, com si fos un butlletí meteorològic breu per a un veí. 
Màxim 150 paraules. Menciona els aspectes més destacats del dia: temperatura, vent si és rellevant, pluja si n'hi ha hagut, i l'estat del cel."""

def genera_resum(context: str) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    missatge = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=PROMPT_SISTEMA,
        messages=[{
            "role": "user",
            "content": f"Genera el resum meteorològic del dia amb aquestes dades:\n\n{context}"
        }]
    )
    return missatge.content[0].text.strip()

# ─── Notifica HA ─────────────────────────────────────────────────────────────

def notifica_ha(resum: str):
    try:
        import requests
        r = requests.post(
            f"{HA_URL}/api/states/sensor.meteo_resum_diari",
            headers={
                "Authorization": f"Bearer {HA_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "state": datetime.now().strftime("%Y-%m-%d"),
                "attributes": {
                    "resum": resum,
                    "friendly_name": "Resum meteorològic diari",
                    "icon": "mdi:weather-partly-cloudy",
                }
            },
            timeout=5
        )
        log.info(f"HA actualitzat: {r.status_code}")
    except Exception as e:
        log.warning(f"No s'ha pogut notificar HA: {e}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Data a resumir (YYYY-MM-DD), per defecte avui")
    parser.add_argument("--force", action="store_true",
                        help="Regenera el resum encara que ja existeixi")
    args = parser.parse_args()

    init_db()

    if not args.force and resum_ja_existeix(args.data):
        log.info(f"Resum del {args.data} ja existeix — usa --force per regenerar")
        return

    dades = recull_dades_dia(args.data)
    if not dades["readings"] and not dades["analisis"]:
        log.warning(f"Sense dades per al {args.data}")
        return

    context = prepara_context(args.data, dades)
    log.info(f"Generant resum per al {args.data}...")
    log.debug(f"Context:\n{context}")

    resum = genera_resum(context)
    desa_resum(args.data, resum)
    notifica_ha(resum)

    log.info(f"Resum generat:\n{resum}")
    print(f"\n{'='*50}\n{resum}\n{'='*50}")

if __name__ == "__main__":
    main()