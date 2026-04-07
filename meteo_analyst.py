#!/usr/bin/env python3
"""
Analitzador meteorològic amb ChatGPT Vision
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
from openai import OpenAI

# ─── Configuració ────────────────────────────────────────────────────────────

BASE_DIR       = Path("/data/meteo")
DB_PATH        = Path("/opt/meteo-analyst/meteo.db")
STATE_FILE     = Path("/opt/meteo-analyst/state.json")
ANALYSE_EVERY  = 6

# Model més econòmic que gpt-4.1 per aquesta tasca
MODEL          = "gpt-4.1-mini"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-analyst.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Constants de validació ──────────────────────────────────────────────────

TIPUS_NUVOLS = {
    "sense núvols", "cúmuls", "estrats", "cirrus",
    "nimboestrats", "cumulonimbus", "no visible"
}

TIPUS_PRECIP = {
    "pluja", "pluja feble", "aiguaneu", "neu", "cap", "no determinat"
}

VISIBILITAT = {"alta", "mitja", "baixa", "molt baixa"}

VENT = {"calma", "lleuger", "moderat", "fort"}

CONDICIO_GENERAL = {
    "assolellat", "parcialment ennuvolat", "ennuvolat",
    "boirós", "plujós", "tempestuós", "nocturn"
}

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
            raw_json         TEXT
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
        timestamp,
        str(imatge),
        dades.get("cobertura_núvols"),
        dades.get("tipus_núvols"),
        int(bool(dades.get("precipitació", False))),
        dades.get("tipus_precipitació"),
        dades.get("visibilitat"),
        dades.get("vent_apparent"),
        dades.get("condició_general"),
        dades.get("observacions"),
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

# ─── Prompt + schema ─────────────────────────────────────────────────────────

PROMPT = """Analitza aquesta imatge d'una estació meteorològica.

Retorna exclusivament un objecte JSON vàlid.
No afegeixis text fora del JSON.
No facis markdown.
No incloguis explicacions.

Criteris:
- "cobertura_núvols": enter de 0 a 100
- "tipus_núvols": classifica només amb una de les opcions permeses
- "precipitació": true o false
- "tipus_precipitació": usa només una de les opcions permeses
- "visibilitat": usa només una de les opcions permeses
- "vent_apparent": inferit visualment de vegetació o objectes
- "condició_general": usa només una de les opcions permeses
- "observacions": text curt, màxim 100 caràcters

Si la imatge és nocturna o molt fosca:
- marca "condició_general" com "nocturn"
- completa la resta de camps amb la millor inferència possible"""

ANALYSIS_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "cobertura_núvols": {
            "type": "integer",
            "minimum": 0,
            "maximum": 100
        },
        "tipus_núvols": {
            "type": "string",
            "enum": [
                "sense núvols", "cúmuls", "estrats", "cirrus",
                "nimboestrats", "cumulonimbus", "no visible"
            ]
        },
        "precipitació": {
            "type": "boolean"
        },
        "tipus_precipitació": {
            "type": "string",
            "enum": ["pluja", "pluja feble", "aiguaneu", "neu", "cap", "no determinat"]
        },
        "visibilitat": {
            "type": "string",
            "enum": ["alta", "mitja", "baixa", "molt baixa"]
        },
        "vent_apparent": {
            "type": "string",
            "enum": ["calma", "lleuger", "moderat", "fort"]
        },
        "condició_general": {
            "type": "string",
            "enum": [
                "assolellat", "parcialment ennuvolat", "ennuvolat",
                "boirós", "plujós", "tempestuós", "nocturn"
            ]
        },
        "observacions": {
            "type": "string",
            "maxLength": 100
        }
    },
    "required": [
        "cobertura_núvols",
        "tipus_núvols",
        "precipitació",
        "tipus_precipitació",
        "visibilitat",
        "vent_apparent",
        "condició_general",
        "observacions"
    ]
}

# ─── Utilitats ───────────────────────────────────────────────────────────────

def guess_media_type(path_imatge: Path) -> str:
    suffix = path_imatge.suffix.lower()
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    return "image/jpeg"

def normalitza_dades(dades: dict) -> dict:
    """
    Ajusta valors límit i comprova enums.
    Si algun camp no és vàlid, llença ValueError.
    """
    if not isinstance(dades, dict):
        raise ValueError("La resposta no és un objecte JSON")

    # cobertura_núvols
    cobertura = dades.get("cobertura_núvols")
    if not isinstance(cobertura, int):
        raise ValueError("cobertura_núvols no és enter")
    dades["cobertura_núvols"] = max(0, min(100, cobertura))

    # precipitació
    if not isinstance(dades.get("precipitació"), bool):
        raise ValueError("precipitació no és booleà")

    # enums
    if dades.get("tipus_núvols") not in TIPUS_NUVOLS:
        raise ValueError(f"tipus_núvols invàlid: {dades.get('tipus_núvols')}")

    if dades.get("tipus_precipitació") not in TIPUS_PRECIP:
        raise ValueError(f"tipus_precipitació invàlid: {dades.get('tipus_precipitació')}")

    if dades.get("visibilitat") not in VISIBILITAT:
        raise ValueError(f"visibilitat invàlida: {dades.get('visibilitat')}")

    if dades.get("vent_apparent") not in VENT:
        raise ValueError(f"vent_apparent invàlid: {dades.get('vent_apparent')}")

    if dades.get("condició_general") not in CONDICIO_GENERAL:
        raise ValueError(f"condició_general invàlida: {dades.get('condició_general')}")

    observacions = dades.get("observacions")
    if not isinstance(observacions, str):
        raise ValueError("observacions no és string")
    dades["observacions"] = observacions[:100]

    return dades

# ─── Anàlisi amb ChatGPT ─────────────────────────────────────────────────────

def analitza_imatge(path_imatge: Path) -> dict:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("No s'ha definit OPENAI_API_KEY")

    client = OpenAI(api_key=api_key)

    image_bytes = path_imatge.read_bytes()
    media_type = guess_media_type(path_imatge)
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    image_data_url = f"data:{media_type};base64,{image_b64}"

    response = client.responses.create(
        model=MODEL,
        input=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": PROMPT
                    },
                    {
                        "type": "input_image",
                        "image_url": image_data_url,
                        "detail": "high"
                    }
                ]
            }
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "analisi_meteorologica",
                "schema": ANALYSIS_SCHEMA,
                "strict": True
            }
        }
    )

    text = response.output_text.strip()
    dades = json.loads(text)
    return normalitza_dades(dades)

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    init_db()
    estat = llegeix_estat()

    latest = BASE_DIR / "latest.jpg"
    if not latest.exists():
        log.warning("No s'ha trobat /data/meteo/latest.jpg — esperant captures")
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

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"Analitzant {latest} ...")

    try:
        dades = analitza_imatge(latest)
        desa_analisi(timestamp, latest, dades)
        log.info(
            f"OK — {dades.get('condició_general')} | "
            f"núvols: {dades.get('cobertura_núvols')}% | "
            f"precipitació: {dades.get('precipitació')}"
        )
    except json.JSONDecodeError as e:
        log.error(f"Error parsejant JSON de ChatGPT: {e}")
        sys.exit(1)
    except ValueError as e:
        log.error(f"Resposta JSON invàlida: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error inesperat: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
