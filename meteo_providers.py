#!/usr/bin/env python3
"""
Mòdul compartit de proveïdors LLM per scripts meteorològics.
Suporta: claude, openai, local (Ollama), gemini

Ús des de qualsevol script:
    from meteo_providers import get_provider, llm_vision, llm_text
"""
import os
import base64
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# ─── Proveïdors disponibles ───────────────────────────────────────────────────

PROVIDERS = ["claude", "openai", "local", "gemini"]

# Models per defecte per proveïdor
DEFAULT_MODELS = {
    "claude": "claude-haiku-4-5-20251001",
    "openai": "gpt-4o-mini",
    "local":  "llava",
    "gemini": "gemini-2.0-flash",
}

# URL per proveïdor local (Ollama)
LOCAL_URL = os.environ.get("LOCAL_LLM_URL", "http://192.168.31.XXX:11434")


def get_provider(provider: str = None) -> str:
    """
    Retorna el proveïdor actiu.
    Ordre de prioritat:
      1. Paràmetre explícit
      2. Variable d'entorn METEO_PROVIDER
      3. 'claude' per defecte
    """
    if provider:
        return provider
    return os.environ.get("METEO_PROVIDER", "claude")


def get_model(provider: str, model: str = None) -> str:
    """Retorna el model a usar per un proveïdor."""
    if model:
        return model
    env_key = f"METEO_MODEL_{provider.upper()}"
    return os.environ.get(env_key, DEFAULT_MODELS.get(provider, ""))


# ─── Vision (imatge → JSON) ───────────────────────────────────────────────────

def llm_vision(image_path: Path, prompt: str,
               provider: str = None, model: str = None) -> dict:
    """
    Envia una imatge + prompt a un LLM i retorna un dict JSON.
    Llança excepció si falla.
    """
    provider = get_provider(provider)
    model    = get_model(provider, model)

    log.debug(f"llm_vision: provider={provider} model={model} img={image_path.name}")

    if provider == "claude":
        return _vision_claude(image_path, prompt, model)
    elif provider == "openai":
        return _vision_openai(image_path, prompt, model)
    elif provider == "local":
        return _vision_local(image_path, prompt, model)
    elif provider == "gemini":
        return _vision_gemini(image_path, prompt, model)
    else:
        raise ValueError(f"Proveïdor desconegut: {provider}. Opcions: {PROVIDERS}")


def llm_text(system: str, user: str,
             provider: str = None, model: str = None,
             max_tokens: int = 500) -> str:
    """
    Envia un prompt de text a un LLM i retorna la resposta com a string.
    """
    provider = get_provider(provider)
    model    = get_model(provider, model)

    log.debug(f"llm_text: provider={provider} model={model}")

    if provider == "claude":
        return _text_claude(system, user, model, max_tokens)
    elif provider == "openai":
        return _text_openai(system, user, model, max_tokens)
    elif provider == "local":
        return _text_local(system, user, model, max_tokens)
    elif provider == "gemini":
        return _text_gemini(system, user, model, max_tokens)
    else:
        raise ValueError(f"Proveïdor desconegut: {provider}. Opcions: {PROVIDERS}")


# ─── Implementacions Claude ───────────────────────────────────────────────────

def _vision_claude(image_path: Path, prompt: str, model: str) -> dict:
    import anthropic
    client     = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    image_data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")

    msg = client.messages.create(
        model=model,
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": image_data,
                    }
                },
                {"type": "text", "text": prompt}
            ]
        }]
    )
    return _parse_json(msg.content[0].text)


def _text_claude(system: str, user: str, model: str, max_tokens: int) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}]
    )
    return msg.content[0].text.strip()


# ─── Implementacions OpenAI ───────────────────────────────────────────────────

def _vision_openai(image_path: Path, prompt: str, model: str) -> dict:
    from openai import OpenAI
    client     = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    image_data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")

    msg = client.chat.completions.create(
        model=model,
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_data}"
                    }
                },
                {"type": "text", "text": prompt}
            ]
        }]
    )
    return _parse_json(msg.choices[0].message.content)


def _text_openai(system: str, user: str, model: str, max_tokens: int) -> str:
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    msg = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ]
    )
    return msg.choices[0].message.content.strip()


# ─── Implementacions Local (Ollama) ───────────────────────────────────────────

def _vision_local(image_path: Path, prompt: str, model: str) -> dict:
    import requests
    image_data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")

    r = requests.post(
        f"{LOCAL_URL}/api/generate",
        json={
            "model":  model,
            "prompt": prompt,
            "images": [image_data],
            "stream": False,
        },
        timeout=60
    )
    r.raise_for_status()
    return _parse_json(r.json()["response"])


def _text_local(system: str, user: str, model: str, max_tokens: int) -> str:
    import requests

    r = requests.post(
        f"{LOCAL_URL}/api/generate",
        json={
            "model":  model,
            "prompt": f"{system}\n\n{user}",
            "stream": False,
        },
        timeout=60
    )
    r.raise_for_status()
    return r.json()["response"].strip()


# ─── Implementacions Gemini ───────────────────────────────────────────────────

def _vision_gemini(image_path: Path, prompt: str, model: str) -> dict:
    """
    Vision via Google Gemini API (google-genai SDK).
    Requereix: pip install google-genai
    Requereix variable d'entorn: GEMINI_API_KEY
    """
    from google import genai
    from google.genai import types

    client     = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    image_data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")

    response = client.models.generate_content(
        model=model,
        contents=[
            types.Content(parts=[
                types.Part(inline_data=types.Blob(
                    mime_type="image/jpeg",
                    data=image_data,
                )),
                types.Part(text=prompt),
            ])
        ],
        config=types.GenerateContentConfig(
            temperature=0.2,
        ),
    )
    return _parse_json(response.text)


def _text_gemini(system: str, user: str, model: str, max_tokens: int) -> str:
    """
    Text via Google Gemini API.
    Requereix: pip install google-genai
    Requereix variable d'entorn: GEMINI_API_KEY
    """
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    response = client.models.generate_content(
        model=model,
        contents=user,
        config=types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=0.3,
        ),
    )
    return response.text.strip()


# ─── Utils ────────────────────────────────────────────────────────────────────

def _parse_json(text: str) -> dict:
    """Neteja i parseja JSON de la resposta d'un LLM."""
    import re
    text = text.strip()
    # Elimina blocs de codi ```json ... ``` o ``` ... ```
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    # Elimina comentaris // fins a final de línia (Gemini els afegeix de vegades)
    text = re.sub(r'//[^\n]*', '', text)
    # Elimina comentaris /* ... */
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
    # Substitueix cometes simples per dobles (amb cura de no trencar contraccions)
    # només quan envolten keys/values JSON
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Últim recurs: agafa només el bloc {} més extern
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            return json.loads(m.group(0))
        raise
