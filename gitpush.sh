#!/bin/bash

set -e  # para si hi ha error

echo "🔍 Comprovant si estem en un repositori git..."
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "❌ No estàs dins d'un repositori git."
    exit 1
fi

echo ""
echo "📋 Estat actual del repositori:"
git status
echo ""

# Mostrar branca actual
BRANCH=$(git branch --show-current)
echo "🌿 Branca actual: $BRANCH"
echo ""

# Confirmació abans de continuar
read -p "❓ Vols continuar amb el commit i push? (y/n): " confirm
if [[ "$confirm" != "y" ]]; then
    echo "🚫 Operació cancel·lada."
    exit 0
fi

# Demanar missatge de commit
read -p "📝 Introdueix el missatge de commit: " commit_msg

if [[ -z "$commit_msg" ]]; then
    echo "❌ El missatge de commit no pot estar buit."
    exit 1
fi

# Afegir canvis
echo ""
echo "➕ Afegint canvis..."
git add -A

# Comprovar si hi ha canvis per commitejar
if git diff --cached --quiet; then
    echo "ℹ️ No hi ha canvis per commitejar."
else
    echo "💾 Fent commit..."
    git commit -m "$commit_msg"
fi

# Push
echo ""
echo "🚀 Fent push a origin/$BRANCH..."
git push origin "$BRANCH"

echo ""
echo "✅ Operació completada correctament."`[6`[6`[6`[6
