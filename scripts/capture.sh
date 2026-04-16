#!/bin/bash
# Captura meteorològica amb timestamp complet al nom posat al proxmox principal 
#i guarda a /meteo que es carrega a la LXC /data/meteo
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE_DIR=$(date +"%Y%m%d")
CAMERA_URL="http://192.168.31.182/capture"
BASE_DIR="/meteo"
OUTPUT_DIR="$BASE_DIR/$DATE_DIR"

mkdir -p "$OUTPUT_DIR"

FILEPATH="$OUTPUT_DIR/snapshot_${TIMESTAMP}.jpg"

if /usr/bin/curl --silent --max-time 10 -o "$FILEPATH" "$CAMERA_URL"; then
    cp "$FILEPATH" "$OUTPUT_DIR/latest.jpg"
    cp "$FILEPATH" "$BASE_DIR/latest.jpg"
    echo "$(date '+%Y-%m-%d %H:%M:%S') OK $FILEPATH"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR captura fallida" >&2
    exit 1
fi