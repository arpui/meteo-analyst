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
import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
import anthropic
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

# ─── Configuració ────────────────────────────────────────────────────────────

DB_PATH  = Path("/data/meteo/meteo.db")
BASE_DIR = Path("/data/meteo")
MODEL    = "claude-haiku-4-5-20251001"

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

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sky_classifications (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           TEXT NOT NULL,
            fitxer              TEXT NOT NULL UNIQUE,
            -- Cobertura
            cel_visible_pct     INTEGER,   -- 0-100% cel sense núvols
            cobertura_pct       INTEGER,   -- 0-100% cel cobert
            -- Color i llum
            color_cel           TEXT,      -- blau intens/blau pàl·lid/blanc lleter/gris/taronja/vermell/negre
            intensitat_llum     TEXT,      -- brillant/normal/difusa/fosca/nocturna
            -- Núvols WMO
            genere_nubol        TEXT,      -- Cumulus/Stratus/Cirrus/Cumulonimbus/
                                           -- Altocumulus/Altostratus/Nimbostratus/
                                           -- Stratocumulus/Cirrostratus/Cirrocumulus
            altura_nubol        TEXT,      -- baixa (<2km)/mitja (2-6km)/alta (>6km)/múltiple
            textura_nubol       TEXT,      -- esponjós/llis/fibrós/massís/floculat/lenticular
            -- Condicions especials
            presencia_boira     INTEGER,   -- 0/1
            presencia_contrail  INTEGER,   -- 0/1 (esteles d'avió)
            -- Qualitat imatge
            imatge_nocturna     INTEGER,   -- 0/1
            qualitat_imatge     TEXT,      -- bona/acceptable/dolenta (soroll, desenfocada...)
            -- Raw
            raw_json            TEXT
        )
    """)
    conn.commit()
    conn.close()

def ja_classificada(fitxer: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT id FROM sky_classifications WHERE fitxer = ?", (str(fitxer),)
    ).fetchone()
    conn.close()
    return row is not None

def desa_classificacio(timestamp: str, fitxer: str, dades: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO sky_classifications (
            timestamp, fitxer,
            cel_visible_pct, cobertura_pct,
            color_cel, intensitat_llum,
            genere_nubol, altura_nubol, textura_nubol,
            presencia_boira, presencia_contrail,
            imatge_nocturna, qualitat_imatge,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        int(dades.get("imatge_nocturna", False)),
        dades.get("qualitat_imatge"),
        json.dumps(dades, ensure_ascii=False)
    ))
    conn.commit()
    conn.close()

# ─── Timestamp del fitxer ─────────────────────────────────────────────────────

def extreu_timestamp(fitxer: Path, date_dir: str) -> str:
    """
    Intenta extreure el timestamp del nom del fitxer.
    Formats suportats:
      - snapshot_20260414_083000.jpg  (nou format)
      - snapshot083000.jpg            (format antic sense data)
      - snapshot_000001.jpg           (format seqüencial, usa mtime)
    Fallback: mtime del fitxer.
    """
    nom = fitxer.stem  # sense extensió

    # Format nou: snapshot_YYYYMMDD_HHMMSS
    m = re.search(r'(\d{8})_(\d{6})', nom)
    if m:
        return datetime.strptime(f"{m.group(1)}_{m.group(2)}", "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

    # Format antic sense data: snapshot083000 (HHMMSS)
    m = re.search(r'snapshot(\d{6})$', nom)
    if m:
        return datetime.strptime(f"{date_dir}_{m.group(1)}", "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

    # Fallback: mtime del fitxer
    mtime = fitxer.stat().st_mtime
    return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

# ─── Prompt WMO ──────────────────────────────────────────────────────────────

PROMPT = """Analitza aquesta imatge meteorològica i retorna ÚNICAMENT un objecte JSON vàlid, sense cap text addicional.

Classifica seguint la nomenclatura WMO (World Meteorological Organization):

{
  "cel_visible_pct": <enter 0-100, percentatge de cel completament lliure de núvols>,
  "cobertura_pct": <enter 0-100, percentatge de cel cobert per núvols>,
  "color_cel": <"blau intens" | "blau pàl·lid" | "blanc lleter" | "gris clar" | "gris fosc" | "taronja" | "vermell" | "negre" | "no determinat">,
  "intensitat_llum": <"brillant" | "normal" | "difusa" | "fosca" | "nocturna">,
  "genere_nubol": <"Cumulus" | "Stratus" | "Cirrus" | "Cumulonimbus" | "Altocumulus" | "Altostratus" | "Nimbostratus" | "Stratocumulus" | "Cirrostratus" | "Cirrocumulus" | "sense núvols" | "no determinat">,
  "altura_nubol": <"baixa (<2km)" | "mitja (2-6km)" | "alta (>6km)" | "múltiple" | "sense núvols" | "no determinat">,
  "textura_nubol": <"esponjós" | "llis" | "fibrós" | "massís" | "floculat" | "lenticular" | "sense núvols" | "no determinat">,
  "presencia_boira": <true | false>,
  "presencia_contrail": <true | false, esteles blanques d'avions>,
  "imatge_nocturna": <true | false>,
  "qualitat_imatge": <"bona" | "acceptable" | "dolenta">,
  "observacions": <string opcional, màxim 80 caràcters, només si hi ha algo molt destacat>
}

Nota: cel_visible_pct + cobertura_pct han de sumar 100."""

# ─── Classificació amb Claude ─────────────────────────────────────────────────

def classifica_imatge(path: Path) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    image_data = base64.standard_b64encode(path.read_bytes()).decode("utf-8")

    missatge = client.messages.create(
        model=MODEL,
        max_tokens=512,
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
                {"type": "text", "text": PROMPT}
            ]
        }]
    )

    text = missatge.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())

# ─── Filtre horari ───────────────────────────────────────────────────────────

HORA_INICI = 7   # hora mínima per considerar foto diürna (conservador)
HORA_FI    = 21  # hora màxima

def es_diurna(fitxer: Path, date_dir: str) -> bool:
    """Retorna True si la foto és de dia segons el timestamp."""
    timestamp = extreu_timestamp(fitxer, date_dir)
    hora = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").hour
    return HORA_INICI <= hora <= HORA_FI

# ─── Recull fitxers a processar ───────────────────────────────────────────────

def fitxers_del_dia(date_dir: str, nomes_diurnes: bool = True) -> list[Path]:
    """Retorna llista de fotos d'un dia (exclou latest.jpg i nocturnes)."""
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
    """Retorna llista de (date_dir, fitxer) dels últims N dies, ordre invers (recent primer)."""
    resultat = []
    avui = datetime.now().date()
    for i in range(0, dies + 1):  # de més recent a més antic
        data = avui - timedelta(days=i)
        date_dir = data.strftime("%Y%m%d")
        for f in fitxers_del_dia(date_dir):
            resultat.append((date_dir, f))
    return resultat

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Classificador del cel i núvols per fotos meteorològiques"
    )
    parser.add_argument("--data",  help="Processa un dia concret (YYYYMMDD)")
    parser.add_argument("--dies",  type=int, default=1,
                        help="Processa els últims N dies (per defecte 1=avui)")
    parser.add_argument("--force", action="store_true",
                        help="Reclassifica encara que ja estigui fet")
    parser.add_argument("--limit", type=int, default=0,
                        help="Límit de fotos a processar (0=sense límit, útil per proves)")
    args = parser.parse_args()

    init_db()

    # Decideix quins fitxers processar
    if args.data:
        parells = [(args.data, f) for f in fitxers_del_dia(args.data)]
    else:
        parells = fitxers_periode(args.dies)

    if not parells:
        log.warning("No s'han trobat fitxers a processar")
        return

    log.info(f"Fitxers trobats: {len(parells)}")

    if args.limit:
        parells = parells[:args.limit]
        log.info(f"Límit aplicat: processant {len(parells)} fotos")

    # Processa
    ok = errors = saltats = 0

    for date_dir, fitxer in parells:
        if not args.force and ja_classificada(fitxer):
            saltats += 1
            continue

        timestamp = extreu_timestamp(fitxer, date_dir)

        try:
            dades = classifica_imatge(fitxer)
            desa_classificacio(timestamp, fitxer, dades)
            log.info(
                f"OK {fitxer.name} | "
                f"cel: {dades.get('cel_visible_pct')}% | "
                f"núvol: {dades.get('genere_nubol')} | "
                f"llum: {dades.get('intensitat_llum')}"
            )
            ok += 1
        except json.JSONDecodeError as e:
            log.error(f"JSON invàlid {fitxer.name}: {e}")
            errors += 1
        except Exception as e:
            log.error(f"Error {fitxer.name}: {e}")
            errors += 1

    log.info(f"Fet — OK: {ok} | Errors: {errors} | Saltats: {saltats}")
    print(f"\nResultat: {ok} classificades, {errors} errors, {saltats} ja existien")

if __name__ == "__main__":
    main()