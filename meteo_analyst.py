#!/usr/bin/env python3
"""
Analitzador meteorològic amb Vision LLM
Processa imatges de l'estació cada N captures i desa resultats a SQLite
Suporta múltiples proveïdors: claude, openai, local, gemini
"""
import argparse
import os
import sys
import json
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")
sys.path.insert(0, "/opt/meteo-analyst")
from meteo_providers import get_provider, llm_vision

# Proveïdor de producció per defecte
PROVIDER_PROD = os.environ.get("METEO_PROVIDER_PROD", "claude")

# ─── Configuració ────────────────────────────────────────────────────────────

HA_WEBHOOK_URL = "http://192.168.31.228:8123/api/webhook/meteo_update"
HA_URL         = "http://192.168.31.228:8123"
DB_PATH        = Path("/data/meteo/meteo.db")
STATE_FILE     = Path("/opt/meteo-analyst/state.json")
ANALYSE_EVERY  = 1
HA_TOKEN       = os.environ.get("HA_TOKEN", "")
STATION        = os.environ.get("METEO_STATION", "torrelles")
BASE_DIR       = Path(f"/data/meteo/{STATION}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-analyst.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Base de dades ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analisis (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            imatge           TEXT NOT NULL,
            cobertura_núvols INTEGER,
            tipus_núvols     TEXT,
            precipitació     INTEGER,
            tipus_precipit   TEXT,
            visibilitat      TEXT,
            vent_apparent    TEXT,
            condició_general TEXT,
            observacions     TEXT,
            provider         TEXT DEFAULT 'claude',
            station          TEXT DEFAULT 'torrelles',
            raw_json         TEXT
        )
    """)
    for col, defval in [
        ("provider", "'claude'"),
        ("station",  "'torrelles'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE analisis ADD COLUMN {col} TEXT DEFAULT {defval}")
            conn.commit()
        except Exception:
            pass
    conn.commit()
    conn.close()

def desa_analisi(timestamp, imatge, dades, provider, station):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO analisis (
            timestamp, imatge,
            cobertura_núvols, tipus_núvols,
            precipitació, tipus_precipit,
            visibilitat, vent_apparent,
            condició_general, observacions,
            provider, station, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        timestamp, str(imatge),
        dades.get("cobertura_núvols"),
        dades.get("tipus_núvols"),
        int(dades.get("precipitació", False)),
        dades.get("tipus_precipitació"),
        dades.get("visibilitat"),
        dades.get("vent_apparent"),
        dades.get("condició_general"),
        dades.get("observacions"),
        provider,
        station,
        json.dumps(dades, ensure_ascii=False)
    ))
    conn.commit()
    conn.close()

# ─── Estat ───────────────────────────────────────────────────────────────────

def llegeix_estat():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"comptador": 0, "última_anàlisi": None}

def desa_estat(estat):
    STATE_FILE.write_text(json.dumps(estat, ensure_ascii=False, indent=2))

# ─── Prompt ───────────────────────────────────────────────────────────────────

PROMPT = """Analitza aquesta imatge d'una estació meteorològica i retorna ÚNICAMENT un objecte JSON vàlid, sense cap text addicional, amb aquests camps:

{
  "cobertura_núvols": <enter 0-100, percentatge aproximat del cel cobert>,
  "tipus_núvols": <"sense núvols" | "cúmuls" | "estrats" | "cirrus" | "nimboestrats" | "cumulonimbus" | "no visible">,
  "precipitació": <true | false>,
  "tipus_precipitació": <"pluja" | "pluja feble" | "aiguaneu" | "neu" | "cap" | "no determinat">,
  "visibilitat": <"alta" | "mitja" | "baixa" | "molt baixa">,
  "vent_apparent": <"calma" | "lleuger" | "moderat" | "fort" — inferit de vegetació o objectes>,
  "condició_general": <"assolellat" | "parcialment ennuvolat" | "ennuvolat" | "boirós" | "plujós" | "tempestuós" | "nocturn">,
  "observacions": <string curt amb qualsevol detall rellevant, màxim 100 caràcters>
}

Si la imatge és nocturna o molt fosca, indica-ho a condició_general i omple els camps que puguis inferir."""

# ─── Notificació HA ──────────────────────────────────────────────────────────

def notifica_ha():
    try:
        r = requests.post(HA_WEBHOOK_URL, timeout=5)
        log.info(f"Webhook HA enviat: {r.status_code}")
    except Exception as e:
        log.warning(f"Webhook HA fallat (no crític): {e}")

def es_de_dia():
    try:
        r = requests.get(
            f"{HA_URL}/api/states/sun.sun",
            headers={"Authorization": f"Bearer {HA_TOKEN}"},
            timeout=5
        )
        elevacio = r.json()["attributes"].get("elevation", 0)
        log.info(f"Elevació solar: {elevacio}°")
        return elevacio > -5
    except Exception as e:
        log.warning(f"No s'ha pogut consultar el sol: {e} — assumint de dia")
        return True

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force",    action="store_true",
                        help="Analitza ara independentment del comptador")
    parser.add_argument("--provider", default=None,
                        help="Proveïdor LLM: claude, openai, gemini, local")
    parser.add_argument("--station",  default=None,
                        help="Estació: torrelles | espui (per defecte: METEO_STATION)")
    args = parser.parse_args()

    provider = get_provider(args.provider) if args.provider else PROVIDER_PROD
    station  = args.station or STATION
    base_dir = Path(f"/data/meteo/{station}")

    log.info(f"Proveïdor: {provider} | Estació: {station}")

    init_db()
    estat = llegeix_estat()

    latest = base_dir / "latest.jpg"
    if not latest.exists():
        log.warning(f"No s'ha trobat {latest} — esperant captures")
        sys.exit(0)

    if not args.force and not es_de_dia():
        log.info("És de nit (elevació solar ≤ 5°) — saltant anàlisi")
        sys.exit(0)

    estat["comptador"] += 1
    log.info(f"Captura #{estat['comptador']} detectada (analitzar cada {ANALYSE_EVERY})")

    if estat["comptador"] < ANALYSE_EVERY:
        desa_estat(estat)
        log.info(f"Saltant anàlisi ({estat['comptador']}/{ANALYSE_EVERY})")
        sys.exit(0)

    estat["comptador"] = 0
    estat["última_anàlisi"] = datetime.now().isoformat()
    desa_estat(estat)
    notifica_ha()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"Analitzant {latest} amb {provider}...")

    try:
        dades = llm_vision(latest, PROMPT, provider=provider)
        desa_analisi(timestamp, latest, dades, provider, station)
        notifica_ha()
        log.info(
            f"OK [{provider}/{station}] — {dades.get('condició_general')} | "
            f"núvols: {dades.get('cobertura_núvols')}% | "
            f"precipitació: {dades.get('precipitació')}"
        )
    except json.JSONDecodeError as e:
        log.error(f"Error parsejant JSON: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error inesperat: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
