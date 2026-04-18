#!/usr/bin/env python3
"""
Classificador del cel i núvols per fotos meteorològiques
Processa fotos históries i noves, enriqueix la BD amb classificacions detallades
Ús: python3 meteo_sky_classifier.py [--data YYYYMMDD] [--dies N] [--force]
"""
import os
import re
import sqlite3
import logging
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")
sys.path.insert(0, "/opt/meteo-analyst")
from meteo_providers import get_provider, get_model, llm_vision

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH  = Path("/data/meteo/meteo.db")
BASE_DIR = Path("/data/meteo")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/meteo-sky-classifier.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Base de dades ───────────────────────────────────────────────────────────

NOVES_COLUMNES = [
    ("precipitacio_visual",  "INTEGER DEFAULT 0"),
    ("neu_visual",           "INTEGER DEFAULT 0"),
    ("gotes_objectiu",       "INTEGER DEFAULT 0"),
    ("objectiu_net",         "TEXT"),
    ("confianca_llm",        "INTEGER"),
    ("confianca_coherencia", "INTEGER"),
    ("confianca_total",      "INTEGER"),
    ("delta_pressio_1h",     "REAL"),
    ("rain_rate_moment",     "REAL"),
    ("temp_moment",          "REAL"),
    ("provider",             "TEXT DEFAULT 'claude'"),
    ("model",                "TEXT DEFAULT 'claude-haiku-4-5-20251001'"),
]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sky_classifications (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp            TEXT NOT NULL,
            fitxer               TEXT NOT NULL,
            cel_visible_pct      INTEGER,
            cobertura_pct        INTEGER,
            color_cel            TEXT,
            intensitat_llum      TEXT,
            genere_nubol         TEXT,
            altura_nubol         TEXT,
            textura_nubol        TEXT,
            presencia_boira      INTEGER DEFAULT 0,
            presencia_contrail   INTEGER DEFAULT 0,
            precipitacio_visual  INTEGER DEFAULT 0,
            neu_visual           INTEGER DEFAULT 0,
            gotes_objectiu       INTEGER DEFAULT 0,
            objectiu_net         TEXT,
            imatge_nocturna      INTEGER DEFAULT 0,
            qualitat_imatge      TEXT,
            confianca_llm        INTEGER,
            confianca_coherencia INTEGER,
            confianca_total      INTEGER,
            delta_pressio_1h     REAL,
            rain_rate_moment     REAL,
            temp_moment          REAL,
            provider             TEXT DEFAULT 'claude',
            model                TEXT DEFAULT 'claude-haiku-4-5-20251001',
            raw_json             TEXT
        )
    """)
    for col, tipus in NOVES_COLUMNES:
        try:
            conn.execute(f"ALTER TABLE sky_classifications ADD COLUMN {col} {tipus}")
            conn.commit()
        except Exception:
            pass
    # Migració columna model
    try:
        conn.execute("ALTER TABLE sky_classifications ADD COLUMN model TEXT DEFAULT 'claude-haiku-4-5-20251001'")
        conn.commit()
    except Exception:
        pass
    # Clau única composta (fitxer + provider + model)
    try:
        conn.execute("DROP INDEX IF EXISTS idx_fitxer_provider")
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fitxer_provider_model
            ON sky_classifications (fitxer, provider, model)
        """)
        conn.commit()
    except Exception:
        pass
    conn.commit()
    conn.close()

def ja_classificada(fitxer: str, provider: str = "claude", model: str = "") -> bool:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT id FROM sky_classifications WHERE fitxer = ? AND provider = ? AND model = ?",
        (str(fitxer), provider, model)
    ).fetchone()
    conn.close()
    return row is not None

def _confianca_total(dades: dict, sensors: dict):
    llm = dades.get("confianca_llm")
    coh = sensors.get("confianca_coherencia")
    if llm is None and coh is None:
        return None
    valors = [v for v in [llm, coh] if v is not None]
    return round(sum(valors) / len(valors))

def desa_classificacio(timestamp: str, fitxer: str, dades: dict,
                        provider: str = "claude", model: str = "",
                        sensors: dict = None):
    sensors = sensors or {}
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO sky_classifications (
            timestamp, fitxer,
            cel_visible_pct, cobertura_pct,
            color_cel, intensitat_llum,
            genere_nubol, altura_nubol, textura_nubol,
            presencia_boira, presencia_contrail,
            precipitacio_visual, neu_visual,
            gotes_objectiu, objectiu_net,
            imatge_nocturna, qualitat_imatge,
            confianca_llm, confianca_coherencia, confianca_total,
            delta_pressio_1h, rain_rate_moment, temp_moment,
            provider, model, raw_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        timestamp, str(fitxer),
        dades.get("cel_visible_pct"),
        dades.get("cobertura_pct"),
        dades.get("color_cel"),
        dades.get("intensitat_llum"),
        dades.get("genere_nubol"),
        dades.get("altura_nubol"),
        dades.get("textura_nubol"),
        int(dades.get("presencia_boira", False)),
        int(dades.get("presencia_contrail", False)),
        int(dades.get("precipitacio_visual", False)),
        int(dades.get("neu_visual", False)),
        int(dades.get("gotes_objectiu", False)),
        dades.get("objectiu_net", "net"),
        int(dades.get("imatge_nocturna", False)),
        dades.get("qualitat_imatge"),
        dades.get("confianca_llm"),
        sensors.get("confianca_coherencia"),
        _confianca_total(dades, sensors),
        sensors.get("delta_pressio_1h"),
        sensors.get("rain_rate_moment"),
        sensors.get("temp_moment"),
        provider,
        model,
        json.dumps(dades, ensure_ascii=False)
    ))
    conn.commit()
    conn.close()

# ─── Sensors del moment ───────────────────────────────────────────────────────

def sensors_del_moment(timestamp: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    row = conn.execute("""
        SELECT * FROM meteo_readings
        WHERE timestamp BETWEEN
            datetime(?, '-10 minutes') AND
            datetime(?, '+10 minutes')
        ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', ?))
        LIMIT 1
    """, (timestamp, timestamp, timestamp)).fetchone()

    row_1h = conn.execute("""
        SELECT pressure_abs FROM meteo_readings
        WHERE timestamp BETWEEN
            datetime(?, '-70 minutes') AND
            datetime(?, '-50 minutes')
        ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', datetime(?, '-1 hour')))
        LIMIT 1
    """, (timestamp, timestamp, timestamp)).fetchone()

    conn.close()

    resultat = {}
    if row:
        resultat["rain_rate_moment"] = row["rain_rate"]
        resultat["temp_moment"]      = row["temp_outdoor"]
        if row_1h and row["pressure_abs"] and row_1h["pressure_abs"]:
            resultat["delta_pressio_1h"] = round(
                row["pressure_abs"] - row_1h["pressure_abs"], 2
            )
    return resultat

def calcula_coherencia(dades: dict, sensors: dict) -> int:
    puntuacio = 100
    rain_rate = sensors.get("rain_rate_moment", 0) or 0
    temp      = sensors.get("temp_moment")
    delta_p   = sensors.get("delta_pressio_1h", 0) or 0
    precipitacio_visual = dades.get("precipitacio_visual", False)
    neu_visual          = dades.get("neu_visual", False)
    cobertura           = dades.get("cobertura_pct", 0) or 0
    intensitat          = dades.get("intensitat_llum", "")
    hora_int = 12
    try:
        hora_int = int(dades.get("_hora_context", "12:00").split(":")[0])
    except Exception:
        pass

    if rain_rate > 1.0 and not precipitacio_visual:
        puntuacio -= 20
    if precipitacio_visual and rain_rate == 0:
        puntuacio -= 15
    if neu_visual and temp is not None and temp > 4:
        puntuacio -= 25
    if intensitat == "brillant" and (hora_int < 8 or hora_int > 19):
        puntuacio -= 20
    if delta_p < -2 and cobertura < 30:
        puntuacio -= 10
    if (hora_int < 6 or hora_int > 22) and not dades.get("imatge_nocturna", False):
        puntuacio -= 30

    return max(0, min(100, puntuacio))

# ─── Timestamp ────────────────────────────────────────────────────────────────

def extreu_timestamp(fitxer: Path, date_dir: str) -> str:
    nom = fitxer.stem
    m = re.search(r'(\d{8})_(\d{6})', nom)
    if m:
        return datetime.strptime(
            f"{m.group(1)}_{m.group(2)}", "%Y%m%d_%H%M%S"
        ).strftime("%Y-%m-%d %H:%M:%S")
    m = re.search(r'snapshot(\d{6})$', nom)
    if m:
        return datetime.strptime(
            f"{date_dir}_{m.group(1)}", "%Y%m%d_%H%M%S"
        ).strftime("%Y-%m-%d %H:%M:%S")
    mtime = fitxer.stat().st_mtime
    return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

# ─── Prompt amb context temporal ─────────────────────────────────────────────

PROMPT_BASE = """Analitza aquesta imatge meteorològica feta a {hora} del {data} i retorna UNICAMENT un objecte JSON valid, sense cap text addicional.

IMPORTANT: Son les {hora}. Tingues en compte que:
- Tonalitats taronges/rosades/grogues poden ser llum rasant d'alba o crepuscle, NO cel lliure
- A aquesta hora {context_hora}
- Distingeix entre gotes/bruticia a l'OBJECTIU de la camera (desenfocades, fixes) i precipitacio REAL al cel

Classifica seguint la nomenclatura WMO:

{{
  "cel_visible_pct": <enter 0-100, % cel lliure de nuvols — NO comptis llum rasant com a cel lliure>,
  "cobertura_pct": <enter 0-100, % cel cobert — si tot es gris uniforme posa 90-100>,
  "color_cel": <"blau intens"|"blau palid"|"blanc lleter"|"gris clar"|"gris fosc"|"taronja"|"vermell"|"negre"|"no determinat">,
  "intensitat_llum": <"brillant"|"normal"|"difusa"|"fosca"|"nocturna">,
  "genere_nubol": <"Cumulus"|"Stratus"|"Cirrus"|"Cumulonimbus"|"Altocumulus"|"Altostratus"|"Nimbostratus"|"Stratocumulus"|"Cirrostratus"|"Cirrocumulus"|"sense nuvols"|"no determinat">,
  "altura_nubol": <"baixa (<2km)"|"mitja (2-6km)"|"alta (>6km)"|"multiple"|"sense nuvols"|"no determinat">,
  "textura_nubol": <"esponjos"|"llis"|"fibros"|"massis"|"floculat"|"lenticular"|"sense nuvols"|"no determinat">,
  "presencia_boira": <true|false>,
  "presencia_contrail": <true|false>,
  "precipitacio_visual": <true|false, veus pluja/neu caient o superficies mullades>,
  "neu_visual": <true|false, veus coberta blanca de neu al terra o turons>,
  "gotes_objectiu": <true|false, hi ha gotes desenfocades A L'OBJECTIU de la camera>,
  "objectiu_net": <"net"|"gotes"|"brut"|"boirina">,
  "imatge_nocturna": <true|false>,
  "qualitat_imatge": <"bona"|"acceptable"|"dolenta">,
  "confianca_llm": <enter 0-100, la teva confianca en aquesta classificacio>,
  "observacions": <string opcional, maxim 80 caracters>
}}

Nota: cel_visible_pct + cobertura_pct han de sumar 100."""

def construeix_prompt(timestamp: str) -> str:
    dt       = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    hora     = dt.strftime("%H:%M")
    data     = dt.strftime("%d/%m/%Y")
    hora_int = dt.hour

    if hora_int < 7:
        ctx = "es de nit o matinada — la llum que veus es artificial o lluna"
    elif hora_int < 9:
        ctx = "es alba — la llum taronja/rosa es el sol sortint, NO es cel lliure"
    elif hora_int < 18:
        ctx = "es ple dia — pots classificar amb normalitat"
    elif hora_int < 20:
        ctx = "es crepuscle — la llum taronja/vermella es el sol ponent, NO es cel lliure"
    else:
        ctx = "es nit — la llum que veus es artificial"

    return PROMPT_BASE.format(hora=hora, data=data, context_hora=ctx)

def classifica_imatge(path: Path, timestamp: str, provider: str = None, model: str = None) -> dict:
    prompt = construeix_prompt(timestamp)
    dades  = llm_vision(path, prompt, provider=provider, model=model)
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    dades["_hora_context"] = dt.strftime("%H:%M")
    return dades

# ─── Filtre horari ────────────────────────────────────────────────────────────

HORA_INICI = 7
HORA_FI    = 21

def es_diurna(fitxer: Path, date_dir: str) -> bool:
    timestamp = extreu_timestamp(fitxer, date_dir)
    hora = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").hour
    return HORA_INICI <= hora <= HORA_FI

def fitxers_del_dia(date_dir: str, nomes_diurnes: bool = True) -> list[Path]:
    dia_path = BASE_DIR / date_dir
    if not dia_path.exists():
        return []
    fitxers = sorted([
        f for f in dia_path.glob("snapshot_*.jpg")
        if "latest" not in f.name
    ])
    if nomes_diurnes:
        fitxers = [f for f in fitxers if es_diurna(f, date_dir)]
    return fitxers

def fitxers_periode(dies: int) -> list[tuple[str, Path]]:
    resultat = []
    avui = datetime.now().date()
    for i in range(0, dies + 1):
        data     = avui - timedelta(days=i)
        date_dir = data.strftime("%Y%m%d")
        for f in fitxers_del_dia(date_dir):
            resultat.append((date_dir, f))
    return resultat

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data",     help="Dia concret (YYYYMMDD)")
    parser.add_argument("--dies",     type=int, default=1)
    parser.add_argument("--force",    action="store_true")
    parser.add_argument("--limit",    type=int, default=0)
    parser.add_argument("--provider", default=None,
                        help="claude|openai|local")
    parser.add_argument("--model", default=None,
                        help="Model específic (sobreescriu METEO_MODEL_<PROVIDER>)")
    args = parser.parse_args()
    args.provider   = get_provider(args.provider)
    args.model_name = get_model(args.provider, args.model)

    init_db()

    if args.data:
        parells = [(args.data, f) for f in fitxers_del_dia(args.data)]
    else:
        parells = fitxers_periode(args.dies)

    if not parells:
        log.warning("No s'han trobat fitxers")
        return

    log.info(f"Fitxers: {len(parells)} | Provider: {args.provider}")

    if args.limit:
        parells = parells[:args.limit]
        log.info(f"Limit: {len(parells)} fotos")

    ok = errors = saltats = 0

    for date_dir, fitxer in parells:
        if not args.force and ja_classificada(fitxer, provider=args.provider, model=args.model_name):
            saltats += 1
            continue

        timestamp = extreu_timestamp(fitxer, date_dir)

        try:
            dades   = classifica_imatge(fitxer, timestamp, provider=args.provider, model=args.model_name)
            sensors = sensors_del_moment(timestamp)
            sensors["confianca_coherencia"] = calcula_coherencia(dades, sensors)
            desa_classificacio(timestamp, fitxer, dades,
                               provider=args.provider, model=args.model_name,
                               sensors=sensors)
            log.info(
                f"OK [{args.provider}/{args.model_name}] {fitxer.name} | "
                f"cel:{dades.get('cel_visible_pct')}% | "
                f"núvol:{dades.get('genere_nubol')} | "
                f"llum:{dades.get('intensitat_llum')} | "
                f"conf:{dades.get('confianca_llm')}/{sensors.get('confianca_coherencia')}"
            )
            ok += 1
        except json.JSONDecodeError as e:
            log.error(f"JSON invalid {fitxer.name}: {e}")
            errors += 1
        except Exception as e:
            log.error(f"Error {fitxer.name}: {e}")
            errors += 1

    log.info(f"Fet — OK:{ok} | Errors:{errors} | Saltats:{saltats}")
    print(f"\nResultat: {ok} classificades, {errors} errors, {saltats} ja existien")

if __name__ == "__main__":
    main()