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

# Versió: 2026-04-19 19:30
load_dotenv("/opt/meteo-analyst/.env")

app = Flask(__name__)

# Proveïdor de producció — determina quin LLM usa HA
PROVIDER_PROD = os.environ.get("METEO_PROVIDER_PROD", "claude")

# BD unificada al directori mountbind (persistent a Debian)
DB_PATH  = Path("/data/meteo/meteo.db")
STATION  = os.environ.get("METEO_STATION", "torrelles")
BASE_DIR = Path(f"/data/meteo/{STATION}")

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
        SELECT * FROM analisis WHERE provider = ? ORDER BY id DESC LIMIT 1
    """, (PROVIDER_PROD,)).fetchone()
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
        WHERE timestamp LIKE ? AND provider = ?
        ORDER BY id DESC
    """, (f"{avui_str}%", PROVIDER_PROD)).fetchall()
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
                "SELECT * FROM analisis WHERE provider = ? ORDER BY id DESC LIMIT 1",
                (PROVIDER_PROD,)
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
    try:
        n_sky = conn.execute("SELECT COUNT(*) FROM sky_classifications").fetchone()[0]
    except Exception:
        n_sky = 0
    conn.close()
    return jsonify({
        "status":      "ok",
        "readings":    n_readings,
        "analisis":    n_analisis,
        "sky_classifications": n_sky,
    })


@app.route("/meteo/foto/<date_dir>/<nom_fitxer>")
def serve_foto(date_dir, nom_fitxer):
    """Serveix una foto per path: /meteo/foto/20260414/snapshot_070001.jpg"""
    foto = BASE_DIR / date_dir / nom_fitxer
    if not foto.exists():
        return "foto no trobada", 404
    return send_file(foto, mimetype="image/jpeg")


@app.route("/meteo/foto/abs/<path:fitxer_path>")
def serve_foto_abs(fitxer_path):
    """Serveix una foto pel path absolut: /meteo/foto/abs/data/meteo/espui/20251227/snap.jpg"""
    foto = Path("/") / fitxer_path
    if not foto.exists():
        return "foto no trobada", 404
    return send_file(foto, mimetype="image/jpeg")


@app.route("/meteo/validacio")
def validacio():
    """
    Pàgina HTML per validar classificacions: foto + dades costat a costat
    ?data=20260414  ?limit=50  ?nocturnes=1  ?tot=1
    """
    from flask import request

    data      = request.args.get("data")
    limit     = int(request.args.get("limit", 20))
    nocturnes = request.args.get("nocturnes", "0") == "1"
    tot       = request.args.get("tot", "0") == "1"
    ordre     = request.args.get("ordre", "temps")  # temps | comparar

    filtres = []
    params  = []

    if data:
        filtres.append("timestamp LIKE ?")
        params.append(f"{data[:4]}-{data[4:6]}-{data[6:8]}%")

    if not tot:
        if not nocturnes:
            filtres.append("imatge_nocturna = 0")
        filtres.append("qualitat_imatge != 'dolenta'")

    where    = f"WHERE {' AND '.join(filtres)}" if filtres else ""
    order_sq = "ORDER BY timestamp ASC, provider ASC" if ordre == "comparar" else "ORDER BY timestamp ASC"

    params.append(limit)

    conn = get_db()

    # Pas 1: sky_classifications filtrades
    sky_rows = conn.execute(f"""
        SELECT * FROM sky_classifications
        {where}
        {order_sq} LIMIT ?
    """, params).fetchall()

    # Pas 2: per cada fila, busca el sensor més proper
    rows = []
    for r in sky_rows:
        m = conn.execute("""
            SELECT temp_outdoor, humidity, pressure_rel, wind_speed, rain_rate
            FROM meteo_readings
            WHERE timestamp BETWEEN
                datetime(?, '-10 minutes') AND
                datetime(?, '+10 minutes')
            ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', ?))
            LIMIT 1
        """, (r["timestamp"], r["timestamp"], r["timestamp"])).fetchone()
        rows.append((r, m))

    conn.close()

    if not rows:
        return "<h2>Sense classificacions encara</h2>", 404

    def build_dades(r, m):
        """Genera el bloc de dades (taula) per una classificació."""
        raw = json.loads(r["raw_json"]) if r["raw_json"] else {}
        obs = raw.get("observacions", "")
        cob = r["cobertura_pct"] or 0
        if cob < 25:   badge_cob = "#27ae60"
        elif cob < 60: badge_cob = "#f39c12"
        else:          badge_cob = "#7f8c8d"
        conf = r["confianca_total"]
        if conf is None:   conf_color = "#555"
        elif conf >= 80:   conf_color = "#27ae60"
        elif conf >= 60:   conf_color = "#f39c12"
        else:              conf_color = "#e74c3c"
        conf_text = f"{conf}%" if conf is not None else "—"
        temp  = f"{m['temp_outdoor']:.1f}°C" if m and m["temp_outdoor"] is not None else "—"
        hum   = f"{m['humidity']:.0f}%" if m and m["humidity"] is not None else "—"
        pres  = f"{m['pressure_rel']:.0f} hPa" if m and m["pressure_rel"] is not None else "—"
        vent  = f"{m['wind_speed']:.1f} km/h" if m and m["wind_speed"] is not None else "—"
        pluja = f"{m['rain_rate']:.1f} mm/h" if m and m["rain_rate"] is not None else "—"
        dp = r["delta_pressio_1h"]
        if dp is not None:
            signe = "+" if dp >= 0 else ""
            dp_color = "#27ae60" if dp > 0.5 else ("#e74c3c" if dp < -0.5 else "#aaa")
            dp_text = f'<span style="color:{dp_color}">{signe}{dp:.1f} hPa/h</span>'
        else:
            dp_text = "—"
        prov  = r["provider"] or "claude"
        mod   = (r["model"] or "").split("/")[-1].replace("claude-","").replace("-20251001","").replace("-2024","")
        provider_badge = f"{prov}/{mod}"
        return f"""
            <div class="fila cap">
                <span class="badge" style="background:{badge_cob}">☁ {cob}% cobert</span>
                <span class="badge" style="background:#2980b9">☀ {r["cel_visible_pct"] or 0}% lliure</span>
                <span class="badge" style="background:{conf_color}">⚡ {conf_text}</span>
                <span class="badge" style="background:#555;font-size:0.7em">{provider_badge}</span>
            </div>
            <table>
                <tr><td>Núvol</td><td><b>{r["genere_nubol"] or "—"}</b> / {r["altura_nubol"] or "—"} / {r["textura_nubol"] or "—"}</td></tr>
                <tr><td>Color / Llum</td><td>{r["color_cel"] or "—"} / {r["intensitat_llum"] or "—"}</td></tr>
                <tr><td>Boira / Contrail</td><td>{("✓ boira" if r["presencia_boira"] else "—")} / {("✓ contrail" if r["presencia_contrail"] else "—")}</td></tr>
                <tr><td>Precipitació / Neu</td><td>{("🌧 sí" if r["precipitacio_visual"] else "—")} / {("❄ sí" if r["neu_visual"] else "—")}</td></tr>
                <tr><td>Objectiu</td><td>{r["objectiu_net"] or "—"} {("💧" if r["gotes_objectiu"] else "")}</td></tr>
                <tr><td>Qualitat</td><td>{r["qualitat_imatge"] or "—"}</td></tr>
                <tr style="background:#0a1628"><td colspan="2" style="color:#7fb3d3;padding-top:5px">⬡ Sensors</td></tr>
                <tr><td>Temp / Hum / Pressió</td><td>{temp} / {hum} / {pres}</td></tr>
                <tr><td>Vent / Pluja sensor</td><td>{vent} / {pluja}</td></tr>
                <tr><td>Δ Pressió 1h</td><td>{dp_text}</td></tr>
                <tr><td>Confiança LLM / Coh.</td><td>{r["confianca_llm"] or "—"} / {r["confianca_coherencia"] or "—"}</td></tr>
                {'<tr><td>Observacions</td><td><i>' + obs + '</i></td></tr>' if obs else ""}
            </table>"""

    if ordre == "comparar":
        from collections import defaultdict
        grups = defaultdict(list)
        for r, m in rows:
            grups[r["fitxer"]].append((r, m))

        cards = ""
        for fitxer_key, classificacions in grups.items():
            fitxer   = Path(fitxer_key)
            date_dir = fitxer.parent.name
            nom      = fitxer.name
            foto_url = f"/meteo/foto/abs{r['fitxer']}"
            ts       = classificacions[0][0]["timestamp"]

            # Sensors (agafem el primer que tingui dades)
            m_sens = next((m for _, m in classificacions if m), None)
            temp  = f"{m_sens['temp_outdoor']:.1f}°C" if m_sens and m_sens["temp_outdoor"] is not None else "—"
            hum   = f"{m_sens['humidity']:.0f}%" if m_sens and m_sens["humidity"] is not None else "—"
            pres  = f"{m_sens['pressure_rel']:.0f} hPa" if m_sens and m_sens["pressure_rel"] is not None else "—"
            vent  = f"{m_sens['wind_speed']:.1f} km/h" if m_sens and m_sens["wind_speed"] is not None else "—"
            pluja = f"{m_sens['rain_rate']:.1f} mm/h" if m_sens and m_sens["rain_rate"] is not None else "—"

            # Columnes per model — header + dades compactes
            cols_header = ""
            cols_dades  = ""
            for r, m in classificacions:
                prov  = r["provider"] or "claude"
                mod   = (r["model"] or "").split("/")[-1].replace("claude-","").replace("-20251001","").replace("-2024","")
                label = f"{prov}/{mod}"
                cob   = r["cobertura_pct"] or 0
                if cob < 25:   bc = "#27ae60"
                elif cob < 60: bc = "#f39c12"
                else:           bc = "#7f8c8d"
                conf  = r["confianca_total"]
                cc    = "#27ae60" if conf and conf>=80 else ("#f39c12" if conf and conf>=60 else "#e74c3c") if conf else "#555"
                ct    = f"{conf}%" if conf else "—"
                dp    = r["delta_pressio_1h"]
                dp_t  = f"{'+ ' if dp and dp>=0 else ''}{dp:.1f}" if dp is not None else "—"

                cols_header += f'<th><span class="badge" style="background:#334">{label}</span></th>'
                cols_dades  += f"""<td>
                    <div style="margin-bottom:4px">
                        <span class="badge" style="background:{bc};font-size:0.7em">☁{cob}%</span>
                        <span class="badge" style="background:{cc};font-size:0.7em">⚡{ct}</span>
                    </div>
                    <div style="font-size:0.75em;line-height:1.6">
                        <b>{r["genere_nubol"] or "—"}</b><br>
                        {r["color_cel"] or "—"} / {r["intensitat_llum"] or "—"}<br>
                        {("🌧" if r["precipitacio_visual"] else "")}
                        {("❄" if r["neu_visual"] else "")}
                        {("🌫" if r["presencia_boira"] else "")}
                        {("💧" if r["gotes_objectiu"] else "")}
                        {("✈" if r["presencia_contrail"] else "")}<br>
                        obj: {r["objectiu_net"] or "—"}<br>
                        Δp: {dp_t} hPa/h<br>
                        LLM:{r["confianca_llm"] or "—"} Coh:{r["confianca_coherencia"] or "—"}
                    </div>
                </td>"""

            cards += f"""
            <div class="card card-comparar">
                <div class="foto">
                    <img src="{foto_url}" alt="{nom}" loading="lazy">
                    <div class="timestamp">{ts}</div>
                </div>
                <div class="dades-multi">
                    <table class="taula-comp">
                        <thead><tr><th></th>{cols_header}</tr></thead>
                        <tbody><tr><td class="label-col">Classificació</td>{cols_dades}</tr></tbody>
                    </table>
                    <div class="sensors-row">
                        🌡 {temp} &nbsp;|&nbsp; 💧 {hum} &nbsp;|&nbsp;
                        🔵 {pres} &nbsp;|&nbsp; 💨 {vent} &nbsp;|&nbsp; 🌧 {pluja}
                    </div>
                </div>
            </div>"""
    else:
        cards = ""
        for r, m in rows:
            fitxer   = Path(r["fitxer"])
            date_dir = fitxer.parent.name
            nom      = fitxer.name
            foto_url = f"/meteo/foto/abs{r['fitxer']}"
            cards += f"""
            <div class="card">
                <div class="foto">
                    <img src="{foto_url}" alt="{nom}" loading="lazy">
                    <div class="timestamp">{r["timestamp"]}</div>
                </div>
                <div class="dades">{build_dades(r, m)}</div>
            </div>"""

    html = f"""<!DOCTYPE html>
<html lang="ca">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Validacio classificacions cel</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background: #1a1a2e; color: #eee; padding: 20px; }}
        h1 {{ text-align: center; margin-bottom: 24px; font-size: 1.4em; color: #a8d8ea; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(580px, 1fr)); gap: 20px; }}
        .card {{ background: #16213e; border-radius: 12px; overflow: hidden;
                 display: flex; flex-direction: row; border: 1px solid #0f3460; }}
        .foto {{ position: relative; width: 240px; min-width: 240px; }}
        .foto img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
        .timestamp {{ position: absolute; bottom: 0; left: 0; right: 0;
                      background: rgba(0,0,0,0.75); font-size: 0.68em;
                      padding: 3px 6px; text-align: center; color: #ccc; }}
        .dades {{ padding: 10px; flex: 1; }}
        .fila.cap {{ display: flex; gap: 6px; margin-bottom: 8px; flex-wrap: wrap; }}
        .badge {{ padding: 3px 8px; border-radius: 20px; font-size: 0.75em;
                  font-weight: bold; color: white; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.78em; }}
        td {{ padding: 2px 5px; border-bottom: 1px solid #0f3460; }}
        td:first-child {{ color: #a8d8ea; width: 42%; }}
        /* Mode comparar */
        .card-comparar {{ align-items: stretch; flex-direction: row; }}
        .dades-multi {{ flex: 1; display: flex; flex-direction: column; }}
        .taula-comp {{ width: 100%; border-collapse: collapse; flex: 1; }}
        .taula-comp th {{ background: #0f3460; padding: 5px 8px; font-size: 0.75em; text-align: center; }}
        .taula-comp td {{ padding: 6px 8px; border: 1px solid #0f3460; vertical-align: top; }}
        .taula-comp .label-col {{ color: #a8d8ea; font-size: 0.75em; width: 60px; }}
        .sensors-row {{ padding: 6px 10px; background: #0a1628; font-size: 0.75em;
                        color: #a8d8ea; border-top: 1px solid #0f3460; }}
    </style>
</head>
<body>
    <h1>🌤 Validacio classificacions — {len(rows)} resultats · mode: {ordre}</h1>
    <div class="grid">{cards}</div>
</body>
</html>"""

    return html


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8765, debug=False)
