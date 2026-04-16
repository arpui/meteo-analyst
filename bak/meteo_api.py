#!/usr/bin/env python3
"""
API REST per exposar dades meteorològiques a Home Assistant
Córrer com a servei systemd al LXC
"""
import os
import sqlite3
import json
import subprocess
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, send_file
from dotenv import load_dotenv

load_dotenv("/opt/meteo-analyst/.env")

app = Flask(__name__)

# BD unificada al directori mountbind (persistent a Debian)
DB_PATH  = Path("/data/meteo/meteo.db")
BASE_DIR = Path("/data/meteo")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Endpoints existents ──────────────────────────────────────────────────────

@app.route("/meteo/latest")
def latest():
    """Última anàlisi visual — el que llegirà Home Assistant"""
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
    avui_str = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT * FROM analisis
        WHERE timestamp LIKE ?
        ORDER BY id DESC
    """, (f"{avui_str}%",)).fetchall()
    conn.close()

    if not rows:
        return jsonify({"error": "sense dades avui"}), 404

    hores_pluja   = sum(1 for r in rows if r["precipitació"])
    mitja_nuvolos = sum(r["cobertura_núvols"] or 0 for r in rows) / len(rows)
    condicions    = [r["condició_general"] for r in rows if r["condició_general"]]
    condicio_freq = max(set(condicions), key=condicions.count) if condicions else None

    return jsonify({
        "data":              avui_str,
        "total_analisis":    len(rows),
        "hores_pluja":       round(hores_pluja * 0.5, 1),
        "mitja_nuvolositat": round(mitja_nuvolos, 1),
        "condició_dominant": condicio_freq,
        "última_anàlisi":   rows[0]["timestamp"],
    })

@app.route("/meteo/historial")
def historial():
    """Últimes 48 anàlisis visuals per a gràfics"""
    conn = get_db()
    rows = conn.execute("""
        SELECT timestamp, condició_general, cobertura_núvols, precipitació, visibilitat
        FROM analisis ORDER BY id DESC LIMIT 48
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/meteo/image")
def image():
    latest = BASE_DIR / "latest.jpg"
    if not latest.exists():
        return "no image", 404
    return send_file(latest, mimetype="image/jpeg")

@app.route("/meteo/analitza", methods=["POST"])
def analitza_ara():
    """HA demana una anàlisi immediata del latest.jpg"""
    try:
        result = subprocess.run(
            ["/opt/meteo-analyst/venv/bin/python3",
             "/opt/meteo-analyst/meteo_analyst.py", "--force"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ}
        )
        if result.returncode == 0:
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

# ─── Nou endpoint combinat ────────────────────────────────────────────────────

@app.route("/meteo/combined/latest")
def combined_latest():
    """
    Última lectura combinada: sensors Ecowitt + anàlisi visual més propera en temps.
    Fa un JOIN aproximat per timestamp (±10 min).
    """
    conn = get_db()

    # Última lectura de sensors
    reading = conn.execute("""
        SELECT * FROM meteo_readings ORDER BY id DESC LIMIT 1
    """).fetchone()

    # Última anàlisi visual
    analisi = conn.execute("""
        SELECT * FROM analisis ORDER BY id DESC LIMIT 1
    """).fetchone()

    conn.close()

    if not reading and not analisi:
        return jsonify({"error": "sense dades"}), 404

    result = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    if reading:
        result["sensors"] = {
            "timestamp":      reading["timestamp"],
            "temperatura":    reading["temp_outdoor"],
            "sensació":       reading["temp_feel"],
            "punt_rosada":    reading["temp_dewpoint"],
            "temp_interior":  reading["temp_indoor"],
            "humitat":        reading["humidity"],
            "humitat_int":    reading["humidity_indoor"],
            "pressio_abs":    reading["pressure_abs"],
            "pressio_rel":    reading["pressure_rel"],
            "vpd":            reading["vpd"],
            "vent_vel":       reading["wind_speed"],
            "vent_ratxa":     reading["wind_gust"],
            "vent_direccio":  reading["wind_direction"],
            "vent_max_dia":   reading["wind_gust_max"],
            "radiacio_solar": reading["solar_radiation"],
            "lux":            reading["solar_lux"],
            "uv":             reading["uv_index"],
            "pluja_rate":     reading["rain_rate"],
            "pluja_hora":     reading["rain_hourly"],
            "pluja_dia":      reading["rain_daily"],
        }

    if analisi:
        result["visio"] = {
            "timestamp":        analisi["timestamp"],
            "condició_general": analisi["condició_general"],
            "cobertura_núvols": analisi["cobertura_núvols"],
            "tipus_núvols":     analisi["tipus_núvols"],
            "precipitació":     bool(analisi["precipitació"]),
            "visibilitat":      analisi["visibilitat"],
            "vent_apparent":    analisi["vent_apparent"],
            "observacions":     analisi["observacions"],
        }

    return jsonify(result)


@app.route("/meteo/combined/avui")
def combined_avui():
    """
    Resum del dia combinant sensors i visió.
    Estadístiques dels sensors + resum visual.
    """
    conn = get_db()
    avui_str = datetime.now().strftime("%Y-%m-%d")

    # Lectures de sensors del dia
    readings = conn.execute("""
        SELECT * FROM meteo_readings
        WHERE timestamp LIKE ?
        ORDER BY id ASC
    """, (f"{avui_str}%",)).fetchall()

    # Anàlisis visuals del dia
    analisis = conn.execute("""
        SELECT * FROM analisis
        WHERE timestamp LIKE ?
        ORDER BY id DESC
    """, (f"{avui_str}%",)).fetchall()

    conn.close()

    result = {"data": avui_str}

    if readings:
        temps  = [r["temp_outdoor"] for r in readings if r["temp_outdoor"] is not None]
        humits = [r["humidity"]     for r in readings if r["humidity"]     is not None]
        vents  = [r["wind_speed"]   for r in readings if r["wind_speed"]   is not None]
        ratxes = [r["wind_gust"]    for r in readings if r["wind_gust"]    is not None]

        result["sensors"] = {
            "total_lectures":  len(readings),
            "temp_min":        round(min(temps),  1) if temps  else None,
            "temp_max":        round(max(temps),  1) if temps  else None,
            "temp_mitja":      round(sum(temps)  / len(temps),  1) if temps  else None,
            "humitat_mitja":   round(sum(humits) / len(humits), 1) if humits else None,
            "vent_mitja":      round(sum(vents)  / len(vents),  1) if vents  else None,
            "ratxa_max":       round(max(ratxes), 1) if ratxes else None,
            "pluja_dia":       readings[-1]["rain_daily"] if readings else None,
            "uv_max":          max((r["uv_index"] for r in readings if r["uv_index"] is not None), default=None),
        }

    if analisis:
        condicions    = [r["condició_general"] for r in analisis if r["condició_general"]]
        condicio_freq = max(set(condicions), key=condicions.count) if condicions else None
        mitja_nuvolos = sum(r["cobertura_núvols"] or 0 for r in analisis) / len(analisis)
        hores_pluja   = sum(1 for r in analisis if r["precipitació"])

        result["visio"] = {
            "total_analisis":    len(analisis),
            "condició_dominant": condicio_freq,
            "mitja_nuvolositat": round(mitja_nuvolos, 1),
            "hores_pluja":       round(hores_pluja * 0.5, 1),
            "última_anàlisi":   analisis[0]["timestamp"],
        }

    return jsonify(result)


@app.route("/health")
def health():
    conn = get_db()
    n_readings = conn.execute("SELECT COUNT(*) FROM meteo_readings").fetchone()[0]
    n_analisis = conn.execute("SELECT COUNT(*) FROM analisis").fetchone()[0]
    conn.close()
    return jsonify({
        "status":      "ok",
        "readings":    n_readings,
        "analisis":    n_analisis,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8765, debug=False)