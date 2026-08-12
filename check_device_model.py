"""
check_device_model.py – Einmaliger, risikofreier Kompatibilitäts-Check

Prüft, welches Modell und welches Protokoll dein Roborock-Konto für das
konfigurierte Gerät tatsächlich meldet, OHNE irgendeine Reinigung
auszulösen oder etwas am Gerät zu verändern. Nutze dieses Skript zuerst,
bevor du q10_fetch_map.py produktiv laufen lässt - besonders wichtig,
wenn du nicht 100% sicher bist, ob dein Gerät ein Q10 (B01-Protokoll,
Modell-Suffix "ss*") oder ein anderes Modell wie der Qrevo (typischerweise
V1-Protokoll) ist.

Voraussetzung: roborock_token.json muss bereits vorhanden sein (einmaliger
manueller Login), secrets.yaml muss email + robot.duid enthalten.

Aufruf:
    python3 check_device_model.py
"""

import asyncio
import json
import os

import yaml

from roborock.data.containers import UserData
from roborock.devices.device_manager import UserParams, create_device_manager
from roborock.web_api import RoborockApiClient

TOKEN_FILE = "roborock_token.json"
SECRETS_FILE = "secrets.yaml"


async def main():
    if not os.path.exists(TOKEN_FILE):
        print(f"FEHLER: '{TOKEN_FILE}' nicht gefunden. Bitte zuerst einmalig einloggen "
              f"(separates Login-Skript).")
        return

    if not os.path.exists(SECRETS_FILE):
        print(f"FEHLER: '{SECRETS_FILE}' nicht gefunden.")
        return

    with open(SECRETS_FILE, "r") as f:
        secrets = yaml.safe_load(f) or {}

    email = secrets.get("email")
    duid = secrets.get("robot", {}).get("duid")

    if not email or not duid:
        print("FEHLER: 'email' und/oder 'robot.duid' fehlen in secrets.yaml.")
        return

    with open(TOKEN_FILE, "r") as f:
        user = UserData.from_dict(json.load(f))

    api = RoborockApiClient(email)
    base_url = await api.base_url
    params = UserParams(username=email, user_data=user, base_url=base_url)

    print("Baue Verbindung auf (nur zum Auslesen, es wird NICHTS gereinigt)...")
    manager = await create_device_manager(params)

    try:
        home_data = await api.get_home_data_v3(user)
        print("\n=== Alle Geräte im Konto ===")
        for dev in getattr(home_data, "devices", []) or []:
            product = next(
                (p for p in getattr(home_data, "products", []) if p.id == dev.product_id),
                None
            )
            print(f"  Name: {dev.name}")
            print(f"    DUID:      {dev.duid}")
            print(f"    Modell:    {getattr(product, 'model', 'unbekannt')}")
            print(f"    Protokoll (pv): {dev.pv}")
            print(f"    {'>>> DAS ist das in secrets.yaml konfigurierte Gerät <<<' if dev.duid == duid else ''}")
            print()

        device = await manager.get_device(duid)
        if not device:
            print(f"FEHLER: Gerät mit DUID '{duid}' wurde nicht gefunden.")
            return

        model = getattr(getattr(device, "product", None), "model", "unbekannt")
        pv = getattr(getattr(device, "device_info", None), "pv", "unbekannt")

        print("=== Konfiguriertes Gerät im Detail ===")
        print(f"Name:      {device.name}")
        print(f"Modell:    {model}")
        print(f"Protokoll: {pv}")
        print()

        model_suffix = str(model).split(".")[-1] if model else ""

        if pv == "B01" and "ss" in model_suffix:
            print("ERGEBNIS: Dies ist ein B01/Q10-kompatibles Gerät (Modell-Suffix "
                  "'ss*'). q10_fetch_map.py sollte für dieses Gerät funktionieren.")
        elif pv == "B01" and "sc" in model_suffix:
            print("ERGEBNIS: Dies ist ein B01/Q7-Gerät (Modell-Suffix 'sc*'), "
                  "KEIN Q10. q10_fetch_map.py wurde für Q10 gebaut und wird für "
                  "dieses Gerät vermutlich NICHT funktionieren (andere Trait-Struktur).")
        elif pv == "1.0":
            print("ERGEBNIS: Dies ist ein V1-Protokoll-Gerät - typisch für neuere "
                  "Modelle wie den Roborock Qrevo. q10_fetch_map.py ist für das "
                  "B01/Q10-Protokoll gebaut und wird für dieses Gerät NICHT "
                  "funktionieren. Für V1-Geräte bräuchte es eine eigene Skript-"
                  "Variante basierend auf device.v1_properties statt "
                  "device.b01_q10_properties.")
        else:
            print(f"ERGEBNIS: Unbekannte Kombination aus Protokoll '{pv}' und "
                  f"Modell '{model}'. Bitte im python-roborock-Repository nach "
                  f"diesem Modell suchen, um die passende Trait-Struktur zu finden.")

    finally:
        await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
