#!/bin/bash
# Processa les fotos d'un dia amb el sky classifier
# Ús: ./processa_dia.sh DD [MM [YYYY]] [--station STATION] [--interval N] [--provider PROV]

# ─── Arguments ───────────────────────────────────────────────────────────────

DIA=$1
MES=${2:-$(date +"%m")}
ANY=${3:-$(date +"%Y")}

# Opcions addicionals passades directament al classifier
shift 3 2>/dev/null || shift $# 2>/dev/null
EXTRA_ARGS="$@"

if [ -z "$DIA" ]; then
    echo "Ús: $0 DD [MM [YYYY]] [--station STATION] [--interval N] [--provider PROV]"
    echo "Exemples:"
    echo "  $0 14                              → 14 del mes i any actuals (torrelles)"
    echo "  $0 14 03                           → 14 de març de l'any actual"
    echo "  $0 14 03 2026                      → 14 de març de 2026"
    echo "  $0 14 03 2026 --station espui      → Espui"
    echo "  $0 14 03 2026 --interval 15        → una foto cada 15min"
    echo "  $0 14 03 2026 --station espui --interval 60 --provider gemini"
    exit 1
fi

# Assegura format de dos dígits
DIA=$(printf "%02d" "$DIA")
MES=$(printf "%02d" "$MES")

DATA="${ANY}${MES}${DIA}"

# ─── Confirmació ─────────────────────────────────────────────────────────────

ORDRE="python3 /opt/meteo-analyst/meteo_sky_classifier.py --data ${DATA} ${EXTRA_ARGS}"

echo ""
echo "  Data a processar : ${DIA}/${MES}/${ANY}"
echo "  Ordre            : ${ORDRE}"
echo ""
read -p "  Confirma amb ENTER per executar (Ctrl+C per cancel·lar)... "

# ─── Execució ─────────────────────────────────────────────────────────────────

echo ""
source /opt/meteo-analyst/venv/bin/activate
$ORDRE
