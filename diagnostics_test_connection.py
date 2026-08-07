"""
Diagnose-Skript: prüft Cloud-Verbindung, Device-Abruf und Property-Start.
Kein Dauerbetrieb - für den produktiven Daemon siehe q10_fetch_map.py.
"""
import asyncio
import json
import os
import yaml
from roborock.data.containers import UserData
from roborock.web_api import RoborockApiClient
from roborock.devices.device_manager import UserParams, create_device_manager

TOKEN_FILE = "roborock_token.json"
SECRETS_FILE = "secrets.yaml"


async def test():
    if not os.path.exists(SECRETS_FILE) or not os.path.exists(TOKEN_FILE):
        print("[FEHLER] secrets.yaml oder roborock_token.json nicht gefunden!")
        return

    with open(SECRETS_FILE, "r") as f:
        secrets = yaml.safe_load(f)

    email = secrets.get("email")
    duid = secrets.get("robot", {}).get("duid")

    if not email:
        print("[FEHLER] Keine E-Mail in secrets.yaml gefunden!")
        return
    if not duid:
        print("[FEHLER] Keine DUID in secrets.yaml gefunden!")
        return

    api = RoborockApiClient(email)

    with open(TOKEN_FILE, "r") as f:
        user_data_dict = json.load(f)
    user = UserData.from_dict(user_data_dict)

    print("[INFO] Verifiziere Token mit der Cloud...")
    await api.get_home_data_v3(user)
    base_url = await api.base_url

    print("[INFO] Starte Device Manager...")
    user_params = UserParams(username=email, user_data=user, base_url=base_url)
    manager = await create_device_manager(user_params)

    try:
        print(f"[INFO] Rufe Gerät mit DUID: {duid} ab...")
        device = await manager.get_device(duid)

        if not device:
            print("[FEHLER] Gerät nicht gefunden.")
            return

        # 1. Channel-Klasse und Modul prüfen
        print("\n--- CHANNEL INFO ---")
        print("CHANNEL:", device._channel.__class__)
        print("MODULE :", device._channel.__class__.__module__)

        # 2. Zustand nach automatischem Connect durch get_device()
        print("\n--- ZUSTAND (bereits verbunden via create_device_manager) ---")
        print("local_channel      :", getattr(device._channel, '_local_channel', 'Nicht vorhanden'))
        print("is_local_connected :", getattr(device._channel, 'is_local_connected', 'Nicht vorhanden'))
        print("is_mqtt_connected  :", getattr(device._channel, 'is_mqtt_connected', 'Nicht vorhanden'))
        print("\n[INFO] device.connect() übersprungen - Gerät ist bereits verbunden.")

        props = device.b01_q10_properties
        await props.start()
        print("[INFO] Properties erfolgreich gestartet.")
        await asyncio.sleep(2)
        await props.close()

    except Exception as e:
        print(f"\n[FEHLER] Aufgetreten: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await manager.close()

        # Übrige Hintergrundtasks (z. B. MQTT idle-unsubscribe Timer) sauber canceln
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        print("\n[INFO] Manager sauber und ohne verbleibende Tasks geschlossen.")


if __name__ == "__main__":
    asyncio.run(test())
