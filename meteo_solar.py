#!/usr/bin/env python3
# Versió: 2026-04-21 13:00
"""
Mòdul de càlcul solar per estacions meteorològiques.
Calcula alba, posta i si una hora és diürna per cada estació.
Usa la llibreria astral.
"""
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from astral import LocationInfo
from astral.sun import sun

# ─── Coordenades de les estacions ────────────────────────────────────────────

ESTACIONS = {
    "torrelles": LocationInfo(
        name="Torrelles de Llobregat",
        region="Catalunya",
        timezone="Europe/Madrid",
        latitude=41.352674,
        longitude=1.954446,
    ),
    "espui": LocationInfo(
        name="Espui",
        region="Catalunya",
        timezone="Europe/Madrid",
        latitude=42.7202,
        longitude=0.8008,
    ),
}

# Marge en minuts abans/després de l'alba/posta per incloure
# llum rasant (crepuscle civil)
MARGE_MINUTS = 30


def get_sol(station: str, dia: date = None) -> dict:
    """
    Retorna dict amb alba, posta i info solar per una estació i data.
    {alba, posta, alba_civil, posta_civil, migdia}
    Totes en hora local (Europe/Madrid).
    """
    if dia is None:
        dia = date.today()

    loc = ESTACIONS.get(station, ESTACIONS["torrelles"])
    tz  = ZoneInfo(loc.timezone)

    s = sun(loc.observer, date=dia, tzinfo=tz)

    return {
        "alba":        s["sunrise"],
        "posta":       s["sunset"],
        "alba_civil":  s["dawn"],
        "posta_civil": s["dusk"],
        "migdia":      s["noon"],
    }


def es_diurna(timestamp: str, station: str = "torrelles",
              marge_min: int = MARGE_MINUTS) -> bool:
    """
    Retorna True si el timestamp és dins de la finestra diürna
    (alba_civil - marge fins posta_civil + marge).
    """
    try:
        dt  = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        tz  = ZoneInfo("Europe/Madrid")
        dt  = dt.replace(tzinfo=tz)
        sol = get_sol(station, dt.date())

        inici = sol["alba_civil"] - timedelta(minutes=marge_min)
        fi    = sol["posta_civil"] + timedelta(minutes=marge_min)

        return inici <= dt <= fi
    except Exception:
        # Si falla el càlcul, fallback conservador: 7h-21h
        hora = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").hour
        return 7 <= hora <= 21


def hores_diurnes(station: str = "torrelles", dia: date = None) -> tuple[int, int]:
    """
    Retorna (hora_inici, hora_fi) aproximades per una estació i data.
    Útil per filtres ràpids per hora.
    """
    try:
        sol   = get_sol(station, dia)
        inici = (sol["alba_civil"] - timedelta(minutes=MARGE_MINUTS)).hour
        fi    = (sol["posta_civil"] + timedelta(minutes=MARGE_MINUTS)).hour
        return max(0, inici), min(23, fi)
    except Exception:
        return 7, 21


if __name__ == "__main__":
    # Test ràpid
    from datetime import date
    for s in ["torrelles", "espui"]:
        sol = get_sol(s)
        hi, hf = hores_diurnes(s)
        print(f"{s}: alba {sol['alba'].strftime('%H:%M')} | "
              f"posta {sol['posta'].strftime('%H:%M')} | "
              f"finestra {hi}h-{hf}h")
