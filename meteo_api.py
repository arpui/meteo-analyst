#!/usr/bin/env python3
"""
API REST per exposar dades meteorològiques a Home Assistant
Córrer com a servei systemd al LXC
"""
import os
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, send_file

app = Flask(__name__)

DB_PATH  = Path("/opt/meteo-analyst/meteo.db")
BASE_DIR = Path("/data/meteo")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/meteo/latest")
def latest():
    """Última anàlisi — el que llegirà Home Assistant"""
    conn = get_db()
    row = conn.execute("""
        SELECT * FROM analisis ORDER BY id DESC LIMIT 1
    """).fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "sense dades"}), 404

    return jsonify({
        "timestamp":        row["timestamp"],
        "condició_general": row["condició_general"],
        "cobertura_núvols": row["cobertura_núvols"],
        "tipus_núvols":     row["tipus_núvols"],
        "precipitació":     bool(row["precipitació"]),
        "tipus_precipit":   row["tipus_precipit"],
        "visibilitat":      row["visibilitat"],
        "vent_apparent":    row["vent_apparent"],
        "observacions":     row["observacions"],
    })

@app.route("/meteo/avui")
def avui():
    """Resum estadístic del dia actual"""
    conn = get_db()
    avui = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT * FROM analisis
        WHERE timestamp LIKE ?
        ORDER BY id DESC
    """, (f"{avui}%",)).fetchall()
    conn.close()

    if not rows:
        return jsonify({"error": "sense dades avui"}), 404

    hores_pluja    = sum(1 for r in rows if r["precipitació"])
    mitja_nuvolos  = sum(r["cobertura_núvols"] or 0 for r in rows) / len(rows)
    condicions     = [r["condició_general"] for r in rows if r["condició_general"]]
    condicio_freq  = max(set(condicions), key=condicions.count) if condicions else None

    return jsonify({
        "data":              avui,
        "total_analisis":    len(rows),
        "hores_pluja":       round(hores_pluja * 0.5, 1),  # cada anàlisi = 30min
        "mitja_nuvolositat": round(mitja_nuvolos, 1),
        "condició_dominant": condicio_freq,
        "última_anàlisi":   rows[0]["timestamp"],
    })

@app.route("/meteo/historial")
def historial():
    """Últimes 48 anàlisis per a gràfics"""
    conn = get_db()
    rows = conn.execute("""
        SELECT timestamp, condició_general, cobertura_núvols, precipitació, visibilitat
        FROM analisis ORDER BY id DESC LIMIT 48
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/meteo/image")
def image():
    latest = BASE_DIR / "latest.jpg"
    if not latest.exists():
        return "no image", 404
    return send_file(latest, mimetype="image/jpeg")

import subprocess

@app.route("/meteo/analitza", methods=["POST"])
def analitza_ara():
    """HA demana una anàlisi immediata del latest.jpg"""
    try:
        result = subprocess.run(
            ["/opt/meteo-analyst/venv/bin/python3",
             "/opt/meteo-analyst/meteo_analyst.py","--force"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ}  # ← passa totes les variables d'entorn
        )
        if result.returncode == 0:
            # Retorna directament l'última anàlisi (acabada de fer)
            conn = get_db()
            row = conn.execute(
                "SELECT * FROM analisis ORDER BY id DESC LIMIT 1"
            ).fetchone()
            conn.close()
            return jsonify({
                "timestamp":        row["timestamp"],
                "condició_general": row["condició_general"],
                "cobertura_núvols": row["cobertura_núvols"],
                "precipitació":     bool(row["precipitació"]),
                "visibilitat":      row["visibilitat"],
                "observacions":     row["observacions"],
            })
        else:
            return jsonify({"error": result.stderr}), 500
    except subprocess.TimeoutExpired:
        return jsonify({"error": "timeout"}), 504

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8765, debug=False)
