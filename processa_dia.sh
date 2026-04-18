#!/bin/bash
# Processa les fotos d'un dia amb el sky classifier
# Ús: ./processa_dia.sh DD [MM [YYYY]]

# ─── Arguments ───────────────────────────────────────────────────────────────

DIA=$1
MES=${2:-$(date +"%m")}
ANY=${3:-$(date +"%Y")}

if [ -z "$DIA" ]; then
    echo "Ús: $0 DD [MM [YYYY]]"
    echo "Exemples:"
    echo "  $0 14          → 14 del mes i any actuals"
    echo "  $0 14 03       → 14 de març de l'any actual"
    echo "  $0 14 03 2026  → 14 de març de 2026"
    exit 1
fi

# Assegura format de dos dígits
DIA=$(printf "%02d" "$DIA")
MES=$(printf "%02d" "$MES")

DATA="${ANY}${MES}${DIA}"

# ─── Confirmació ─────────────────────────────────────────────────────────────

ORDRE="python3 /opt/meteo-analyst/meteo_sky_classifier.py --data ${DATA}"

echo ""
echo "  Data a processar : ${DIA}/${MES}/${ANY}"
echo "  Ordre            : ${ORDRE}"
echo ""
read -p "  Confirma amb ENTER per executar (Ctrl+C per cancel·lar)... "

# ─── Execució ─────────────────────────────────────────────────────────────────

echo ""
source /opt/meteo-analyst/venv/bin/activate
$ORDRE
