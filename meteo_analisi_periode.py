#!/usr/bin/env python3
"""
Anàlisi meteorològica d'un període de N dies
Mostra evolució i tendències dels paràmetres més destacats
Ús: python3 meteo_analisi_periode.py --dies N
"""
import os
import sqlite3
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import anthropic
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH  = Path("/data/meteo/meteo.db")
HA_URL   = "http://192.168.31.228:8123"
HA_TOKEN = os.environ.get("HA_TOKEN", "")
MODEL    = "claude-haiku-4-5-20251001"
UBICACIO = "Torrelles de Llobregat"  # ← canvia aquí la teva ubicació

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-analisi-periode.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Recull dades per dia ─────────────────────────────────────────────────────

def resum_dia(conn, data: str) -> dict | None:
    """Calcula estadístiques d'un dia a partir de les dues taules."""
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

    if not readings and not analisis:
        return None

    resum = {"data": data}

    if readings:
        temps  = [r["temp_outdoor"] for r in readings if r["temp_outdoor"] is not None]
        humits = [r["humidity"]     for r in readings if r["humidity"]     is not None]
        vents  = [r["wind_speed"]   for r in readings if r["wind_speed"]   is not None]
        ratxes = [r["wind_gust"]    for r in readings if r["wind_gust"]    is not None]
        prabs  = [r["pressure_abs"] for r in readings if r["pressure_abs"] is not None]
        uvs    = [r["uv_index"]     for r in readings if r["uv_index"]     is not None]
        rads   = [r["solar_radiation"] for r in readings if r["solar_radiation"] is not None]

        resum["sensors"] = {
            "n_lectures":   len(readings),
            "temp_min":     round(min(temps),  1) if temps  else None,
            "temp_max":     round(max(temps),  1) if temps  else None,
            "temp_mitja":   round(sum(temps)  / len(temps),  1) if temps  else None,
            "humitat_mitja":round(sum(humits) / len(humits), 1) if humits else None,
            "vent_mitja":   round(sum(vents)  / len(vents),  1) if vents  else None,
            "ratxa_max":    round(max(ratxes), 1) if ratxes else None,
            "pressio_inici":round(prabs[0],    1) if prabs  else None,
            "pressio_fi":   round(prabs[-1],   1) if prabs  else None,
            "pressio_mitja":round(sum(prabs)  / len(prabs), 1) if prabs  else None,
            "uv_max":       round(max(uvs),    1) if uvs    else None,
            "rad_max":      round(max(rads),   1) if rads   else None,
            "pluja_dia":    readings[-1]["rain_daily"],
        }

    if analisis:
        condicions    = [r["condició_general"] for r in analisis if r["condició_general"]]
        condicio_freq = max(set(condicions), key=condicions.count) if condicions else None
        mitja_nuvolos = sum(r["cobertura_núvols"] or 0 for r in analisis) / len(analisis)
        hores_pluja   = sum(1 for r in analisis if r["precipitació"]) * 0.5

        resum["visio"] = {
            "n_analisis":        len(analisis),
            "condicio_dominant": condicio_freq,
            "nuvolositat_mitja": round(mitja_nuvolos, 1),
            "hores_pluja":       hores_pluja,
        }

    return resum

def recull_periode(dies: int) -> list[dict]:
    """Retorna llista de resums diaris dels últims N dies (sense avui)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    ahir = datetime.now().date() - timedelta(days=1)
    resums = []

    for i in range(dies - 1, -1, -1):  # de més antic a més recent
        data = (ahir - timedelta(days=i)).strftime("%Y-%m-%d")
        r = resum_dia(conn, data)
        if r:
            resums.append(r)

    conn.close()
    return resums

# ─── Prepara context per Claude ───────────────────────────────────────────────

def prepara_context(resums: list[dict]) -> str:
    lines = [f"Període analitzat: {resums[0]['data']} → {resums[-1]['data']} ({len(resums)} dies)\n"]

    for r in resums:
        lines.append(f"── {r['data']} ──")
        s = r.get("sensors", {})
        v = r.get("visio",   {})

        if s:
            lines.append(
                f"  Temp: {s.get('temp_min')}°C / {s.get('temp_max')}°C "
                f"(mitja {s.get('temp_mitja')}°C)"
            )
            lines.append(
                f"  Humitat: {s.get('humitat_mitja')}%  |  "
                f"Vent mitja: {s.get('vent_mitja')} km/h  ratxa màx: {s.get('ratxa_max')} km/h"
            )
            if s.get("pressio_inici") and s.get("pressio_fi"):
                delta_p = round(s["pressio_fi"] - s["pressio_inici"], 1)
                signe   = "+" if delta_p >= 0 else ""
                lines.append(
                    f"  Pressió: {s.get('pressio_mitja')} hPa  "
                    f"(variació dia: {signe}{delta_p} hPa)"
                )
            if s.get("uv_max"):
                lines.append(f"  UV màx: {s.get('uv_max')}  |  Rad. solar màx: {s.get('rad_max')} W/m²")
            pluja = s.get("pluja_dia")
            if pluja:
                lines.append(f"  Pluja acumulada: {pluja} mm")

        if v:
            lines.append(
                f"  Cel: {v.get('condicio_dominant')}  |  "
                f"Nuvolositat: {v.get('nuvolositat_mitja')}%  |  "
                f"Hores pluja: {v.get('hores_pluja')}h"
            )
        lines.append("")

    return "\n".join(lines)

# ─── Crida a Claude ───────────────────────────────────────────────────────────

def genera_analisi(context: str, dies: int) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    prompt_sistema = f"""Ets un meteoròleg que analitza l'evolució del temps a {UBICACIO} (Barcelona).
Escriu en català, de forma clara i natural.
Màxim 250 paraules. Estructura la resposta en:
1. Visió general del període
2. Paràmetres més destacats i la seva evolució (tendències, canvis significatius)
3. Dia més destacat (el més calorós, fred, ventós, etc.)
Evita repetir les dades en brut — interpreta-les i explica què signifiquen."""

    missatge = client.messages.create(
        model=MODEL,
        max_tokens=500,
        system=prompt_sistema,
        messages=[{
            "role": "user",
            "content": (
                f"Analitza l'evolució meteorològica dels últims {dies} dies "
                f"amb aquestes dades:\n\n{context}"
            )
        }]
    )
    return missatge.content[0].text.strip()

# ─── Notifica HA ─────────────────────────────────────────────────────────────

def notifica_ha(analisi: str, dies: int):
    try:
        import requests
        r = requests.post(
            f"{HA_URL}/api/states/sensor.meteo_analisi_periode",
            headers={
                "Authorization": f"Bearer {HA_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "state": f"{dies}d",
                "attributes": {
                    "analisi":       analisi,
                    "dies":          dies,
                    "friendly_name": f"Anàlisi meteorològica {dies} dies",
                    "icon":          "mdi:chart-line",
                }
            },
            timeout=5
        )
        log.info(f"HA actualitzat: {r.status_code}")
    except Exception as e:
        log.warning(f"No s'ha pogut notificar HA: {e}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Anàlisi meteorològica d'un període de N dies"
    )
    parser.add_argument(
        "--dies", type=int, default=7,
        help="Nombre de dies a analitzar (per defecte 7, sense comptar avui)"
    )
    args = parser.parse_args()

    if args.dies < 2:
        log.error("Mínim 2 dies per fer una anàlisi d'evolució")
        return

    log.info(f"Recollint dades dels últims {args.dies} dies...")
    resums = recull_periode(args.dies)

    if not resums:
        log.warning("Sense dades per al període sol·licitat")
        return

    if len(resums) < args.dies:
        log.warning(
            f"Només hi ha dades per a {len(resums)} dels {args.dies} dies sol·licitats"
        )

    context = prepara_context(resums)
    log.debug(f"Context preparat:\n{context}")

    log.info(f"Generant anàlisi del període ({len(resums)} dies)...")
    analisi = genera_analisi(context, len(resums))

    notifica_ha(analisi, args.dies)

    print(f"\n{'='*60}")
    print(f"ANÀLISI METEOROLÒGICA — últims {len(resums)} dies")
    print(f"{'='*60}")
    print(analisi)
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
