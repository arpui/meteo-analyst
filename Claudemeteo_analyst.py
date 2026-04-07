#!/usr/bin/env python3
"""
Analitzador meteorològic amb Claude Vision
Processa imatges de l'estació cada N captures i desa resultats a SQLite
"""

import os
import sys
import json
import base64
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import anthropic

# ─── Configuració ────────────────────────────────────────────────────────────

BASE_DIR       = Path("/data/meteo")
DB_PATH        = Path("/opt/meteo-analyst/meteo.db")
STATE_FILE     = Path("/opt/meteo-analyst/state.json")   # comptador de captures
ANALYSE_EVERY  = 1                                        # N captures entre anàlisis
#MODEL          = "claude-opus-4-20250514"
#MODEL = "claude-opus-4-5"
MODEL = "claude-haiku-4-5-20251001"
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
            cobertura_núvols INTEGER,   -- 0-100
            tipus_núvols     TEXT,
            precipitació     INTEGER,   -- 0/1
            tipus_precipit   TEXT,
            visibilitat      TEXT,      -- alta/mitja/baixa
            vent_apparent    TEXT,      -- calma/moderat/fort (per moviment vegetació)
            condició_general TEXT,
            observacions     TEXT,
            raw_json         TEXT       -- resposta completa de Claude
        )
    """)
    conn.commit()
    conn.close()

def desa_analisi(timestamp, imatge, dades):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO analisis (
            timestamp, imatge,
            cobertura_núvols, tipus_núvols,
            precipitació, tipus_precipit,
            visibilitat, vent_apparent,
            condició_general, observacions, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        json.dumps(dades, ensure_ascii=False)
    ))
    conn.commit()
    conn.close()

# ─── Estat (comptador de captures) ───────────────────────────────────────────

def llegeix_estat():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"comptador": 0, "última_anàlisi": None}

def desa_estat(estat):
    STATE_FILE.write_text(json.dumps(estat, ensure_ascii=False, indent=2))

# ─── Anàlisi amb Claude ───────────────────────────────────────────────────────

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

def analitza_imatge(path_imatge: Path) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    image_data = base64.standard_b64encode(path_imatge.read_bytes()).decode("utf-8")

    missatge = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": image_data
                    }
                },
                {
                    "type": "text",
                    "text": PROMPT
                }
            ]
        }]
    )

    text = missatge.content[0].text.strip()
    # Neteja per si Claude afegeix ```json ... ```
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    init_db()
    estat = llegeix_estat()

    # Troba la imatge més recent (latest.jpg global)
    latest = BASE_DIR / "latest.jpg"
    if not latest.exists():
        log.warning("No s'ha trobat /data/meteo/latest.jpg — esperant captures")
        sys.exit(0)

    # Incrementa comptador
    estat["comptador"] += 1
    log.info(f"Captura #{estat['comptador']} detectada (analitzar cada {ANALYSE_EVERY})")

    if estat["comptador"] < ANALYSE_EVERY:
        desa_estat(estat)
        log.info(f"Saltant anàlisi ({estat['comptador']}/{ANALYSE_EVERY})")
        sys.exit(0)

    # Toca analitzar
    estat["comptador"] = 0
    estat["última_anàlisi"] = datetime.now().isoformat()
    desa_estat(estat)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"Analitzant {latest} ...")

    try:
        dades = analitza_imatge(latest)
        desa_analisi(timestamp, latest, dades)
        log.info(f"OK — {dades.get('condició_general')} | núvols: {dades.get('cobertura_núvols')}% | precipitació: {dades.get('precipitació')}")
    except json.JSONDecodeError as e:
        log.error(f"Error parsejant JSON de Claude: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error inesperat: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
