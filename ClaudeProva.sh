cd /opt/meteo-analyst
source venv/bin/activate
source .env

python3 - <<'EOF'
import anthropic, os

print("API key present:", bool(os.environ.get("ANTHROPIC_API_KEY")))
print("Key prefix:", os.environ.get("ANTHROPIC_API_KEY", "")[:15])

client = anthropic.Anthropic()
print("Client creat OK")

try:
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        messages=[{"role": "user", "content": "di ok"}]
    )
    print("Resposta:", msg.content[0].text)
except Exception as e:
    print("Error tipus:", type(e).__name__)
    print("Error detall:", str(e))
EOF
