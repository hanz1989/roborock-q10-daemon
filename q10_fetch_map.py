"""
Roborock Q10 – Dauerhafter MQTT- & Steuerungsdienst (Resilienz-Edition)

Dieser Dienst vertraut auf die interne Reconnect-Logik der roborock-Bibliothek.
Das Skript baut den DeviceManager auf und fungiert danach als Watchdog:
Nur wenn das Gerät 45 Minuten am Stück auf keine Ping-Anfragen (refresh) reagiert,
wird ein "Hard Reset" des kompletten DeviceManagers erzwungen.

WICHTIG: Dieser Daemon führt NIEMALS eigenmächtig einen interaktiven Login
mit input()/stdin durch. Ein gültiger roborock_token.json muss VOR dem Start
des Diensts durch ein separates, manuell ausgeführtes Login-Skript erzeugt
worden sein.

==============================================================================
WICHTIGER HINWEIS ZUR GERÄTE-KOMPATIBILITÄT (Q10 vs. Qrevo & Co.)
==============================================================================
Dieses Skript ist AUSSCHLIESSLICH für Geräte gebaut, die von python-roborock
über das B01-Protokoll mit Modell-Suffix "ss*" angesprochen werden
(z. B. roborock.vacuum.ss07 = Q10). Neuere/andere Modelle wie der Roborock
Qrevo laufen typischerweise über das modernere V1-Protokoll der Bibliothek
und haben eine GRUNDLEGEND ANDERE Trait-/API-Struktur
(device.v1_properties statt device.b01_q10_properties).

Falls dein tatsächliches Gerät kein B01/Q10 ist, wird dieser Daemon beim
Verbindungsaufbau mit einer klaren Fehlermeldung abbrechen (siehe
connect_device()) statt mit einem kryptischen Absturz - er versucht aber
NICHT, ein V1-Gerät zu unterstützen.

Nutze bei Unsicherheit zuerst das beiliegende `check_device_model.py`, um
zu sehen, welches Protokoll/Modell dein Konto tatsächlich meldet, BEVOR du
diesen Daemon produktiv laufen lässt.
==============================================================================

NEU in dieser Version (Resilienz gegen Cloud-Ausfälle / Sperrungen):

1. Home-Assistant-Anbindung via MQTT Discovery
   - Der Daemon meldet sich automatisch als Gerät in Home Assistant an
     (binary_sensor "Verbindung", sensor "Letzte Meldung").
   - LWT (Last Will) auf dem Availability-Topic sorgt dafür, dass HA sofort
     "nicht verfügbar" anzeigt, wenn der Prozess selbst abstürzt oder der
     Pi/das Netz ausfällt - unabhängig von Roborock-spezifischen Fehlern.

2. Alarmierung via MQTT -> Home Assistant Automation -> Push-Notification
   - Kritische Ereignisse (Token ungültig, Hard-Reset nötig/fehlgeschlagen,
     Reauth nötig/erfolgreich, Geräte-Inkompatibilität) werden auf
     roborock/buksi/alert (retain=True) veröffentlicht. Eine HA-Automation
     kann darauf mit notify.* reagieren.

3. Offline-Cache für HomeData
   - Die zuletzt erfolgreich abgerufenen HomeData (inkl. Geräte-/Raumdaten)
     werden zusätzlich zum In-Memory-Cache auf die Platte gespiegelt
     (home_data_cache.pkl). Schlägt der Cloud-Abruf fehl (z. B. weil
     Roborock die API geändert hat oder kurzfristig blockt), fällt der
     Daemon auf den letzten bekannten Stand zurück, statt komplett zu
     scheitern.

4. Reauth-Fallback ohne SSH
   - Wird der Token ungültig (z. B. weil Roborock die Token-Lebensdauer
     verkürzt hat), fordert der Daemon selbstständig einen neuen Login-Code
     an und wartet bis zu 10 Minuten auf einen Code, der über das MQTT-Topic
     roborock/buksi/reauth_code hereinkommt. In Home Assistant genügt dafür
     ein einfaches "text"-Helper-Feld + eine Automation, die den eingegebenen
     Wert auf dieses Topic publiziert - kein SSH-Login nötig.
     Die Token-Invalid-Erkennung nutzt zusätzlich zur nativen Bibliotheks-
     Exception eine Text-/Typ-Heuristik als Sicherheitsnetz (siehe
     _looks_like_invalid_token), falls die installierte Version keine eigene
     RoborockInvalidUserAgreement-Exception exportiert.

5. Mehrere Räume pro Befehl
   - MQTT-Payload auf roborock/buksi/command akzeptiert eine einzelne
     Raum-ID ("9") ODER eine kommagetrennte Liste ("9,3,5"). Die Bibliothek
     unterstützt clean_segments() nativ mit einer Liste von Raum-IDs -
     das war in der Vorversion durch die Payload-Verarbeitung künstlich
     auf einen Raum pro Aufruf beschränkt, nicht durch die Bibliothek selbst.

6. Konfigurierbare Reinigungseinstellungen (statt hartcodiert)
   - fan_level und clean_mode kommen jetzt aus secrets.yaml (Abschnitt
     "cleaning"), mit Validierung und Fallback auf den bisherigen Standard
     (TURBO / VACUUM).
   - Optional (EXPERIMENTELL, nicht live verifiziert): clean_line steuert
     die Reinigungslinien-Dichte (fast/daily/fine - vermutlich die
     "einfach/doppelt"-Bahnen-Einstellung aus der App). Standardmäßig
     deaktiviert (null in secrets.yaml) - das Gerät behält dann die zuletzt
     in der App eingestellte Dichte bei. Bei Aktivierung: ersten Lauf
     unbedingt beobachten.

==============================================================================
HINWEIS ZUR ENTSTEHUNG
==============================================================================
Dieses Skript wurde iterativ mit Unterstützung mehrerer LLMs entwickelt
(Architektur, Debugging, Dokumentation, Bibliotheks-Introspektion). Es wird
offen so im zugehörigen GitHub-Repository kommuniziert. Vor produktivem
Dauerbetrieb wird trotzdem eine eigene Prüfung/ein eigenes Verständnis des
Codes empfohlen, insbesondere der Abschnitte zu Auth/Reauth und den oben
genannten, nicht live-verifizierten Funktionen (clean_line).
==============================================================================

Setup-Hinweise für Home Assistant (kurz):
  - MQTT-Integration in HA einrichten (falls nicht vorhanden), Broker wie in
    secrets.yaml angegeben.
  - Discovery-Entitäten erscheinen automatisch unter "Buksi Automation
    Daemon" (kein YAML nötig).
  - Für den Reauth-Fallback: Helper "input_text.buksi_reauth_code" anlegen
    und eine Automation, die bei Änderung dieses Helpers den Wert auf
    roborock/buksi/reauth_code published.
  - Für Push-Benachrichtigungen: Automation auf state_changed von
    sensor.buksi_letzte_meldung (bzw. direkt auf das MQTT-Topic
    roborock/buksi/alert), die notify.mobile_app_<dein_handy> aufruft.
"""

import asyncio
import json
import logging
import os
import pickle
import time
import yaml
import paho.mqtt.client as mqtt

from roborock.data.containers import UserData
from roborock.data.b01_q10.b01_q10_code_mappings import B01_Q10_DP, YXCleanLine, YXCleanType, YXFanLevel
from roborock.devices.device_manager import UserParams, create_device_manager
from roborock.exceptions import RoborockTooFrequentCodeRequests
from roborock.web_api import RoborockApiClient

# Merkt sich, ob der native Bibliotheks-Import geklappt hat. Wird beim
# Startup einmalig geloggt, damit im Log sofort sichtbar ist, ob wir uns
# auf die native Exception oder die Heuristik verlassen.
_NATIVE_TOKEN_EXCEPTION_AVAILABLE = True

try:
    # Falls die roborock-Bibliothek einen eigenen Typ für ungültige/abgelaufene
    # Tokens exportiert, nutzen wir den direkt.
    from roborock.exceptions import RoborockInvalidUserAgreement as TokenInvalidError  # type: ignore
except ImportError:
    _NATIVE_TOKEN_EXCEPTION_AVAILABLE = False

    # Fallback: eigener Marker-Typ. Wird von login() explizit geworfen,
    # wenn die Token-Validierung eindeutig auf einen ungültigen/abgelaufenen
    # Token hindeutet (statt auf ein transientes Netzwerkproblem).
    class TokenInvalidError(Exception):
        """Wird geworfen, wenn der gespeicherte Token ungültig/abgelaufen ist
        und ein manueller Reauth erforderlich ist."""
        pass


# Schlüsselwörter, die typischerweise auf einen Auth-/Token-Fehler hindeuten
# (statt auf einen transienten Netzwerk-/Serverfehler).
_TOKEN_ERROR_KEYWORDS = (
    "invalid user agreement",
    "invalid token",
    "invalid session",
    "token expired",
    "token invalid",
    "expired token",
    "unauthorized",
    "unauthenticated",
    "login required",
    "please login",
    "auth failed",
    "authentication failed",
    "code number is wrong",
    "2001",
    "9002",
    " 401",
)


def _looks_like_invalid_token(exc: Exception) -> bool:
    """Heuristische Zweitprüfung für den Fall, dass die installierte
    roborock-Bibliothek keine eigene RoborockInvalidUserAgreement-Exception
    exportiert. Prüft Exception-Klassenname und Fehlertext auf typische
    Auth-/Token-Schlüsselwörter. Bewusst konservativ: ein Nicht-Treffer wird
    weiterhin als transienter Fehler behandelt und normal retried."""
    class_name = type(exc).__name__.lower()
    message = str(exc).lower()

    if "invaliduseragreement" in class_name or "tokeninvalid" in class_name:
        return True

    return any(keyword in message for keyword in _TOKEN_ERROR_KEYWORDS)


# ----------------------------------------------------------------------
# Logging für den Dauerbetrieb
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TOKEN_FILE = "roborock_token.json"
SECRETS_FILE = "secrets.yaml"
HOME_DATA_CACHE_FILE = "home_data_cache.pkl"

# --- Home-Assistant / MQTT Discovery Konfiguration ---
HA_DISCOVERY_PREFIX = "homeassistant"
HA_DEVICE_UNIQUE_ID = "roborock_buksi_daemon"
HA_DEVICE_INFO = {
    "identifiers": [HA_DEVICE_UNIQUE_ID],
    "name": "Buksi Automation Daemon",
    "manufacturer": "Selbstbau (Raspberry Pi)",
    "model": "roborock.vacuum.ss07 Watchdog",
}

REAUTH_WAIT_TIMEOUT = 600  # Sekunden, die auf einen Reauth-Code gewartet wird

# Globale Instanzen
manager = None
props = None
vacuum_obj = None
device_obj = None
loop = None
mqtt_client = None
cleaning_lock = None
connection_lock = None
status_topic = "roborock/buksi/status"
connectivity_topic = "roborock/buksi/connectivity"
alert_topic = "roborock/buksi/alert"
availability_topic = "roborock/buksi/availability"
reauth_code_topic = "roborock/buksi/reauth_code"

# Config
_api = None
_email = None
_duid = None

# Konfigurierbare Reinigungseinstellungen (werden in main() aus secrets.yaml
# gelesen). Defaults entsprechen dem bisherigen hartcodierten Verhalten.
_clean_mode: YXCleanType = YXCleanType.VACUUM
_fan_level: YXFanLevel = YXFanLevel.TURBO
_clean_line: YXCleanLine | None = None  # None = nicht antasten (EXPERIMENTELL wenn gesetzt)

# Reauth-Fallback-State
_reauth_event = None
_pending_reauth_code = None

# Wird gesetzt, wenn ein Cleaning-RPC in einen Timeout gelaufen ist.
# Der Watchdog reagiert danach beim naechsten Zyklus aggressiver
# (siehe health_check_loop).
connection_failure_hint = False

# --- HomeData-Cache: Verhindert unnötige Cloud-Requests beim Hard-Reset ---
_original_get_home_data_v3 = RoborockApiClient.get_home_data_v3
_cached_home_data = None


def _save_home_data_cache(data):
    try:
        with open(HOME_DATA_CACHE_FILE, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        logging.debug("Konnte HomeData-Cache nicht auf Platte schreiben: %s", e)


def _load_home_data_cache():
    if os.path.exists(HOME_DATA_CACHE_FILE):
        try:
            with open(HOME_DATA_CACHE_FILE, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.debug("Konnte HomeData-Cache nicht von Platte laden: %s", e)
    return None


async def _patched_get_home_data_v3(self, user_data=None):
    global _cached_home_data
    if _cached_home_data is not None:
        logging.debug("Cache-Hit (Memory): nutze zwischengespeicherte HomeData v3.")
        return _cached_home_data

    try:
        logging.info("Cloud-Abruf: lade frische HomeData v3 von den Roborock-Servern...")
        result = await _original_get_home_data_v3(self, user_data)
        _cached_home_data = result
        _save_home_data_cache(result)
        return result
    except (TokenInvalidError, RoborockTooFrequentCodeRequests):
        raise
    except Exception as e:
        disk_cache = _load_home_data_cache()
        if disk_cache is not None:
            logging.warning(
                "HomeData-Abruf fehlgeschlagen (%s) - nutze Offline-Cache von Platte.", e
            )
            publish_alert(
                "warning",
                f"Cloud-Abruf für Gerätedaten fehlgeschlagen, nutze Offline-Cache: {e}"
            )
            _cached_home_data = disk_cache
            return disk_cache
        raise


RoborockApiClient.get_home_data_v3 = _patched_get_home_data_v3


def load_secrets():
    if os.path.exists(SECRETS_FILE):
        with open(SECRETS_FILE, "r") as f:
            return yaml.safe_load(f) or {}
    return {}


def _resolve_enum_choice(enum_cls, raw_value, default_member, setting_name):
    """Löst einen String aus secrets.yaml in ein Enum-Mitglied auf (z. B.
    "TURBO" -> YXFanLevel.TURBO). Bei fehlendem oder ungültigem Wert wird
    der Default verwendet und eine Warnung mit allen gültigen Optionen
    geloggt, statt das Skript mit einem KeyError abstürzen zu lassen."""
    if not raw_value:
        return default_member
    try:
        return enum_cls[str(raw_value).upper()]
    except KeyError:
        valid = ", ".join(m.name for m in enum_cls if m.name != "UNKNOWN")
        logging.warning(
            "Ungültiger Wert '%s' für '%s' in secrets.yaml. Gültige Optionen: %s. "
            "Verwende Standard: %s.",
            raw_value, setting_name, valid, default_member.name
        )
        return default_member


# ----------------------------------------------------------------------
# Home-Assistant-Anbindung (MQTT Discovery, Alerts, Connectivity)
# ----------------------------------------------------------------------

def publish_ha_discovery():
    """Meldet die Entitäten per MQTT-Discovery bei Home Assistant an."""
    if not mqtt_client:
        return

    connectivity_config = {
        "name": "Verbindung",
        "unique_id": f"{HA_DEVICE_UNIQUE_ID}_connectivity",
        "state_topic": connectivity_topic,
        "device_class": "connectivity",
        "payload_on": "ON",
        "payload_off": "OFF",
        "availability_topic": availability_topic,
        "payload_available": "online",
        "payload_not_available": "offline",
        "device": HA_DEVICE_INFO,
    }
    alert_config = {
        "name": "Letzte Meldung",
        "unique_id": f"{HA_DEVICE_UNIQUE_ID}_last_alert",
        "state_topic": alert_topic,
        "value_template": "{{ value_json.message }}",
        "json_attributes_topic": alert_topic,
        "icon": "mdi:message-alert",
        "availability_topic": availability_topic,
        "payload_available": "online",
        "payload_not_available": "offline",
        "device": HA_DEVICE_INFO,
    }
    status_config = {
        "name": "Reinigungsstatus",
        "unique_id": f"{HA_DEVICE_UNIQUE_ID}_clean_status",
        "state_topic": status_topic,
        "value_template": "{{ value_json.state }}",
        "json_attributes_topic": status_topic,
        "icon": "mdi:robot-vacuum",
        "availability_topic": availability_topic,
        "payload_available": "online",
        "payload_not_available": "offline",
        "device": HA_DEVICE_INFO,
    }

    mqtt_client.publish(
        f"{HA_DISCOVERY_PREFIX}/binary_sensor/{HA_DEVICE_UNIQUE_ID}/connectivity/config",
        json.dumps(connectivity_config), retain=True
    )
    mqtt_client.publish(
        f"{HA_DISCOVERY_PREFIX}/sensor/{HA_DEVICE_UNIQUE_ID}/last_alert/config",
        json.dumps(alert_config), retain=True
    )
    mqtt_client.publish(
        f"{HA_DISCOVERY_PREFIX}/sensor/{HA_DEVICE_UNIQUE_ID}/clean_status/config",
        json.dumps(status_config), retain=True
    )
    logging.info("Home-Assistant-Discovery-Konfiguration veröffentlicht.")


def publish_alert(level, message):
    """Veröffentlicht eine Meldung, auf die eine HA-Automation reagieren kann
    (z. B. Push-Benachrichtigung via notify.*)."""
    logging.log(
        logging.ERROR if level in ("error", "critical") else
        logging.WARNING if level == "warning" else logging.INFO,
        "[ALERT:%s] %s", level, message
    )
    if mqtt_client:
        try:
            mqtt_client.publish(
                alert_topic,
                json.dumps({
                    "level": level,
                    "message": message,
                    "timestamp": int(time.time()),
                }),
                retain=True
            )
        except Exception as e:
            logging.debug("Konnte Alert nicht per MQTT senden: %s", e)


def publish_connectivity(is_connected: bool):
    if mqtt_client:
        try:
            mqtt_client.publish(connectivity_topic, "ON" if is_connected else "OFF", retain=True)
        except Exception as e:
            logging.debug("Konnte Connectivity-Status nicht senden: %s", e)


# ----------------------------------------------------------------------
# Login / Reauth
# ----------------------------------------------------------------------

async def login(api: RoborockApiClient) -> UserData:
    """
    Validiert einen bestehenden Token. Führt selbst KEINEN interaktiven
    stdin-Login durch (kein input()).

    - Kein Token vorhanden           -> RuntimeError, manueller Ersteinrichtung nötig
    - Token eindeutig ungültig       -> TokenInvalidError (native Exception ODER
                                         per _looks_like_invalid_token()-Heuristik
                                         erkannt), wird vom Aufrufer ggf. über
                                         attempt_ha_guided_reauth() aufgefangen
    - Netzwerk/Cloud kurz nicht da   -> Exception wird durchgereicht,
                                         Startup-/Watchdog-Retry versucht es später erneut
    """
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError(
            "Kein Token vorhanden. Einmaliger manueller Login "
            "(separates Login-Skript) erforderlich."
        )

    with open(TOKEN_FILE, "r") as f:
        user_data_dict = json.load(f)

    try:
        user = UserData.from_dict(user_data_dict)
        await api.get_home_data_v3(user)
        return user

    except TokenInvalidError:
        logging.error("Roborock-Token ungültig oder abgelaufen (native Exception).")
        raise

    except RoborockTooFrequentCodeRequests:
        logging.error("Rate-Limit der Roborock-Cloud aktiv.")
        raise

    except (TimeoutError, ConnectionError, OSError) as e:
        logging.warning("Roborock Cloud temporär nicht erreichbar: %s", e)
        raise

    except Exception as e:
        if _looks_like_invalid_token(e):
            logging.error(
                "Roborock-Token vermutlich ungültig/abgelaufen (per Heuristik "
                "erkannt, da keine native TokenInvalidError-Exception vorlag): "
                "%s: %s", type(e).__name__, e
            )
            raise TokenInvalidError(str(e)) from e

        logging.exception("Unbekannter Fehler bei Token-Verifikation: %s", e)
        raise


async def attempt_ha_guided_reauth(api: RoborockApiClient) -> UserData:
    """
    Fallback, wenn der gespeicherte Token ungültig geworden ist. Fordert
    selbstständig einen neuen Login-Code an und wartet bis zu
    REAUTH_WAIT_TIMEOUT Sekunden auf einen Code, der über MQTT-Topic
    `reauth_code_topic` hereinkommt (z. B. aus einem Home-Assistant
    input_text-Helper). Kein SSH-Zugriff nötig.

    HINWEIS: request_code()/code_login() sind die zum Zeitpunkt der
    Erstellung dieses Skripts gängigen Methodennamen in der roborock-
    Bibliothek für den E-Mail-Code-Login. Falls die installierte Version
    andere Namen verwendet, hier anpassen (z. B. per `dir(api)` prüfen).
    """
    global _pending_reauth_code, _reauth_event

    publish_alert(
        "critical",
        "Roborock-Token ungültig. Reauth erforderlich - bitte Code aus der "
        "E-Mail in Home Assistant eintragen (siehe Buksi-Helper)."
    )

    try:
        await api.request_code(_email)
    except RoborockTooFrequentCodeRequests:
        publish_alert(
            "error",
            "Zu viele Code-Anfragen bei Roborock - bitte später erneut versuchen."
        )
        raise
    except Exception as e:
        publish_alert("error", f"Konnte keinen neuen Login-Code anfordern: {e}")
        raise TokenInvalidError(f"request_code fehlgeschlagen: {e}")

    _pending_reauth_code = None
    _reauth_event.clear()
    logging.info(
        "Warte bis zu %ds auf Reauth-Code über MQTT-Topic '%s'...",
        REAUTH_WAIT_TIMEOUT, reauth_code_topic
    )

    try:
        await asyncio.wait_for(_reauth_event.wait(), timeout=REAUTH_WAIT_TIMEOUT)
    except asyncio.TimeoutError:
        publish_alert(
            "error",
            f"Kein Reauth-Code innerhalb von {REAUTH_WAIT_TIMEOUT}s über Home "
            "Assistant empfangen. Daemon versucht es beim nächsten Zyklus erneut."
        )
        raise TokenInvalidError("Timeout beim Warten auf Reauth-Code")

    code = _pending_reauth_code
    try:
        user = await api.code_login(_email, code)
    except Exception as e:
        publish_alert("error", f"Reauth mit übermitteltem Code fehlgeschlagen: {e}")
        raise TokenInvalidError(f"code_login fehlgeschlagen: {e}")

    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(user.as_dict(), f)
    except Exception as e:
        logging.warning("Neuer Token konnte nicht persistiert werden: %s", e)

    publish_alert("info", "Reauth über Home Assistant erfolgreich! Daemon läuft wieder normal.")
    return user


# ----------------------------------------------------------------------
# Geräteverbindung
# ----------------------------------------------------------------------

async def connect_device():
    """
    Baut den DeviceManager auf. Die Bibliothek übernimmt das connect() und start()
    im Hintergrund automatisch (inkl. eigener Endlos-Reconnect-Schleife).
    Wir rufen hier NICHT mehr manuell connect() oder start() auf!

    Enthält eine explizite Kompatibilitätsprüfung: Falls das Gerät kein
    B01/Q10-Trait bereitstellt (device.b01_q10_properties ist None), bricht
    die Funktion mit einer klaren, sprechenden Fehlermeldung ab - inkl.
    Hinweis, falls die Bibliothek stattdessen ein V1-Trait erkannt hat
    (typisch für neuere Modelle wie den Qrevo). Das verhindert einen
    kryptischen Absturz weiter unten im Code.
    """
    global manager, props, vacuum_obj, device_obj, _api, _email, _duid

    if manager:
        try:
            await manager.close()
        except Exception as e:
            logging.debug("Fehler beim Schließen des alten Managers: %s", e)

    try:
        user = await login(_api)
    except TokenInvalidError:
        user = await attempt_ha_guided_reauth(_api)

    base_url = await _api.base_url
    user_params = UserParams(username=_email, user_data=user, base_url=base_url)

    logging.info("Erstelle DeviceManager (Bibliothek startet Hintergrund-Connect)...")
    new_manager = await create_device_manager(user_params)
    device = await new_manager.get_device(_duid)

    if not device:
        await new_manager.close()
        raise RuntimeError(f"Gerät mit DUID '{_duid}' wurde im Konto nicht gefunden!")

    device_model = getattr(getattr(device, "product", None), "model", "unbekannt")
    device_pv = getattr(getattr(device, "device_info", None), "pv", "unbekannt")
    logging.info(
        "Gerät gefunden: '%s' (Modell: %s, Protokoll: %s)",
        device.name, device_model, device_pv
    )

    # Wir warten kurz, bis der asynchrone Hintergrund-Task der Bibliothek verbunden ist
    connected = False
    for i in range(15):
        if getattr(device, "is_connected", False):
            connected = True
            break
        await asyncio.sleep(1)

    if not connected:
        logging.warning("is_connected ist nach 15s immer noch False. Die interne Schleife versucht es weiter.")

    new_props = device.b01_q10_properties
    if new_props is None:
        hint = ""
        if getattr(device, "v1_properties", None) is not None:
            hint = (
                " Die Bibliothek hat stattdessen ein V1-Trait erkannt - das ist "
                "typisch für neuere Modelle wie den Roborock Qrevo. Dieses Skript "
                "unterstützt AUSSCHLIESSLICH B01/Q10-Geräte (z. B. "
                "roborock.vacuum.ss07) und wurde absichtlich nicht für V1-Geräte "
                "erweitert. Bitte mit check_device_model.py das tatsächliche "
                "Modell/Protokoll verifizieren."
            )
        msg = (
            f"Kein B01/Q10-Trait für Gerät '{device.name}' (Modell: {device_model}, "
            f"Protokoll: {device_pv}) verfügbar.{hint}"
        )
        await new_manager.close()
        logging.critical(msg)
        publish_alert("critical", msg)
        raise RuntimeError(msg)

    new_vacuum = getattr(new_props, "vacuum", None)
    if not new_vacuum:
        await new_manager.close()
        raise RuntimeError("vacuum_obj konnte nicht ermittelt werden.")

    manager = new_manager
    props = new_props
    vacuum_obj = new_vacuum
    device_obj = device

    publish_connectivity(True)
    logging.info("Buksi erfolgreich initialisiert. Überlasse Connection-Handling der Bibliothek.")


async def execute_cleaning(room_ids: list[int]):
    """
    Führt die Raumreinigung für eine oder mehrere Raum-IDs aus.

    Locking: connection_lock liegt AUSSEN um cleaning_lock (siehe
    ausführliche Erklärung in vorherigen Versionen). Der eigentliche
    RPC-Call (clean_segments) ist mit einem Timeout abgesichert.
    """
    global vacuum_obj, mqtt_client, cleaning_lock, connection_lock
    global status_topic, device_obj, connection_failure_hint, props

    async with connection_lock:
        async with cleaning_lock:
            if not device_obj or not vacuum_obj:
                logging.error("Kein Device-Objekt vorhanden.")
                return

            logging.info(
                "is_connected-Status (informativ, kein Abbruchkriterium): %s",
                getattr(device_obj, "is_connected", "unbekannt")
            )

            logging.info("Starte Reinigung für Raum-ID(s): %s", room_ids)

            if mqtt_client and status_topic:
                mqtt_client.publish(
                    status_topic,
                    json.dumps({"state": "started", "rooms": room_ids}),
                    retain=True
                )

            try:
                logging.info("Setze Reinigungsmodus: %s...", _clean_mode.name)
                try:
                    await vacuum_obj.set_clean_mode(_clean_mode)
                except Exception as ex:
                    logging.warning("Konnte Reinigungsmodus nicht setzen: %s", ex)

                logging.info("Setze Saugstufe: %s...", _fan_level.name)
                try:
                    await vacuum_obj.set_fan_level(_fan_level)
                except Exception as ex:
                    logging.warning("Konnte Saugstufe nicht setzen: %s", ex)

                if _clean_line is not None:
                    # EXPERIMENTELL / NICHT LIVE VERIFIZIERT: dpCleanLine wird in
                    # keiner der offiziellen Trait-Methoden gesetzt, existiert aber
                    # als beschreibbares DP im Protokoll. Wird über den öffentlichen
                    # CommandTrait (props.command) angesprochen, nicht über private
                    # Interna. Fehler hier sind nicht fatal für den Putzvorgang.
                    logging.info(
                        "EXPERIMENTELL: Setze Reinigungslinien-Dichte auf %s...",
                        _clean_line.name
                    )
                    try:
                        await props.command.send(
                            command=B01_Q10_DP.CLEAN_LINE,
                            params=_clean_line.code,
                        )
                    except Exception as ex:
                        logging.warning(
                            "Konnte clean_line nicht setzen (experimentell, "
                            "evtl. nicht unterstützt): %s", ex
                        )

                await asyncio.sleep(1)

                await asyncio.wait_for(
                    vacuum_obj.clean_segments([int(r) for r in room_ids]),
                    timeout=30
                )
                logging.info("Reinigungsbefehl erfolgreich an Buksi übertragen!")
                connection_failure_hint = False

            except asyncio.TimeoutError:
                logging.error(
                    "clean_segments() Timeout nach 30s - Gerät antwortet "
                    "vermutlich nicht. Setze Verdachtsmarker für Watchdog."
                )
                connection_failure_hint = True
                if mqtt_client and status_topic:
                    mqtt_client.publish(
                        status_topic,
                        json.dumps({"state": "error", "rooms": room_ids, "error": "timeout"}),
                        retain=True
                    )

            except Exception as e:
                logging.error("Reinigung fehlgeschlagen: %s", e)
                if mqtt_client and status_topic:
                    mqtt_client.publish(
                        status_topic,
                        json.dumps({"state": "error", "rooms": room_ids, "error": str(e)}),
                        retain=True
                    )


def on_mqtt_message(client, userdata, msg):
    global _pending_reauth_code

    if msg.topic == reauth_code_topic:
        code = msg.payload.decode("utf-8").strip()
        if code:
            logging.info("Reauth-Code über MQTT empfangen.")
            _pending_reauth_code = code
            if loop and loop.is_running() and _reauth_event:
                loop.call_soon_threadsafe(_reauth_event.set)
        return

    logging.info("MQTT Nachricht im Callback empfangen.")
    try:
        payload = msg.payload.decode("utf-8").strip()
        if not payload:
            return

        # Unterstützt sowohl eine einzelne Raum-ID ("9") als auch eine
        # kommagetrennte Liste ("9,3,5") für mehrere Räume in einem Befehl.
        room_ids = [int(part.strip()) for part in payload.split(",") if part.strip()]

        if not room_ids:
            raise ValueError("Keine gültige Raum-ID in Payload gefunden")
        if any(r < 1 for r in room_ids):
            raise ValueError("Ungültige Raum-ID (muss >= 1 sein)")

        logging.info("Raum-ID(s) erkannt: %s", room_ids)

        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(execute_cleaning(room_ids), loop)

    except ValueError as ve:
        logging.error("Ungültige Payload: %s", ve)
    except Exception as e:
        logging.error("Fehler bei MQTT-Nachricht: %s", e)


async def health_check_loop():
    """
    Watchdog: Sendet alle 15 Min ein refresh(). Vertraut auf die interne Reconnect-
    Schleife der Bibliothek. Normalerweise wird erst nach 3 Checks in Folge
    (45 Min tot) ein Hard-Reset forciert.
    """
    global device_obj, props, connection_failure_hint
    consecutive_failures = 0
    MAX_FAILURES = 3

    while True:
        await asyncio.sleep(900)  # 15 Minuten

        effective_max = 1 if connection_failure_hint else MAX_FAILURES

        success = False
        if device_obj is not None and props is not None:
            try:
                await asyncio.wait_for(props.refresh(), timeout=20)
                s = props.status
                logging.info(
                    "Watchdog OK – Batterie: %s | Status: %s",
                    getattr(s, "battery", "N/A"),
                    getattr(s, "status", "N/A")
                )
                success = True
            except Exception as e:
                logging.warning("Watchdog: refresh() fehlgeschlagen (%s).", type(e).__name__)
        else:
            logging.warning("Watchdog: Objekte fehlen.")

        if success:
            if consecutive_failures > 0:
                logging.info("Verbindung hat sich im Hintergrund erholt (interner Reconnect).")
                publish_alert("info", "Verbindung zu Buksi hat sich von selbst erholt.")
            consecutive_failures = 0
            connection_failure_hint = False
            publish_connectivity(True)
        else:
            consecutive_failures += 1
            logging.warning(
                "Fehlschlag %d/%d registriert%s.",
                consecutive_failures, effective_max,
                " (verschärfte Schwelle wegen vorherigem Cleaning-Timeout)"
                if connection_failure_hint else ""
            )
            publish_connectivity(False)

            if consecutive_failures >= effective_max:
                logging.error("Verbindung offenbar dauerhaft gestört. Erzwinge Hard-Reset!")
                publish_alert(
                    "warning",
                    f"Verbindung zu Buksi seit {consecutive_failures} Checks gestört. "
                    "Erzwinge Hard-Reset des DeviceManagers."
                )
                async with connection_lock:
                    try:
                        await connect_device()
                        consecutive_failures = 0
                        connection_failure_hint = False
                        publish_alert("info", "Hard-Reset erfolgreich, Verbindung wiederhergestellt.")
                    except TokenInvalidError:
                        logging.error(
                            "Hard-Reset fehlgeschlagen: Token ungültig, auch Reauth "
                            "über Home Assistant hat nicht funktioniert. Daemon bleibt "
                            "im Fehlerzustand und versucht es beim nächsten Zyklus erneut."
                        )
                        publish_connectivity(False)
                    except Exception as e:
                        logging.error("Hard-Reset fehlgeschlagen: %s", e)
                        publish_alert("error", f"Hard-Reset fehlgeschlagen: {e}")
                        publish_connectivity(False)


async def main():
    global loop, mqtt_client, cleaning_lock, connection_lock, status_topic
    global _api, _email, _duid, manager, _reauth_event
    global _clean_mode, _fan_level, _clean_line

    loop = asyncio.get_running_loop()
    cleaning_lock = asyncio.Lock()
    connection_lock = asyncio.Lock()
    _reauth_event = asyncio.Event()

    if _NATIVE_TOKEN_EXCEPTION_AVAILABLE:
        logging.info(
            "Token-Invalid-Erkennung: native RoborockInvalidUserAgreement-"
            "Exception verfügbar."
        )
    else:
        logging.warning(
            "Token-Invalid-Erkennung: native RoborockInvalidUserAgreement-"
            "Exception NICHT gefunden - verlasse mich auf Text-/Typ-Heuristik "
            "(_looks_like_invalid_token)."
        )

    secrets = load_secrets()
    _email = secrets.get("email")
    if not _email:
        logging.error("Keine E-Mail in secrets.yaml gefunden!")
        return

    robot_conf = secrets.get("robot", {})
    _duid = robot_conf.get("duid")
    if not _duid:
        raise RuntimeError("Keine Roboter DUID in secrets.yaml gefunden!")

    mqtt_conf = secrets.get("mqtt", {})
    mqtt_broker = mqtt_conf.get("broker", "192.168.178.148")
    mqtt_port = mqtt_conf.get("port", 1883)
    mqtt_topic_cmd = mqtt_conf.get("topic", "roborock/buksi/command")
    status_topic = mqtt_conf.get("status_topic", "roborock/buksi/status")

    # --- Konfigurierbare Reinigungseinstellungen ---
    cleaning_conf = secrets.get("cleaning", {})
    _clean_mode = _resolve_enum_choice(
        YXCleanType, cleaning_conf.get("clean_mode"), YXCleanType.VACUUM, "cleaning.clean_mode"
    )
    _fan_level = _resolve_enum_choice(
        YXFanLevel, cleaning_conf.get("fan_level"), YXFanLevel.TURBO, "cleaning.fan_level"
    )
    clean_line_raw = cleaning_conf.get("clean_line")
    if clean_line_raw:
        try:
            _clean_line = YXCleanLine[str(clean_line_raw).upper()]
            logging.warning(
                "clean_line ist aktiviert (%s) - dies ist EXPERIMENTELL und "
                "nicht live verifiziert. Ersten Reinigungslauf bitte beobachten.",
                _clean_line.name
            )
        except KeyError:
            valid = ", ".join(m.name for m in YXCleanLine if m.name != "UNKNOWN")
            logging.warning(
                "Ungültiger clean_line-Wert '%s'. Gültige Optionen: %s. Wird ignoriert.",
                clean_line_raw, valid
            )
            _clean_line = None
    else:
        _clean_line = None

    logging.info(
        "Reinigungseinstellungen: Modus=%s | Saugstufe=%s | Linien-Dichte=%s",
        _clean_mode.name, _fan_level.name,
        _clean_line.name if _clean_line else "unverändert (App-Einstellung)"
    )

    _api = RoborockApiClient(_email)

    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.on_message = on_mqtt_message
        mqtt_client.will_set(availability_topic, payload="offline", retain=True)
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)
        mqtt_client.subscribe(mqtt_topic_cmd)
        mqtt_client.subscribe(reauth_code_topic)
        mqtt_client.loop_start()
        mqtt_client.publish(availability_topic, "online", retain=True)
        publish_ha_discovery()
        publish_connectivity(False)
    except Exception as e:
        logging.error("MQTT-Setup fehlgeschlagen: %s. Fahre ohne HA-Anbindung fort.", e)

    startup_attempt = 0
    while True:
        startup_attempt += 1
        try:
            logging.info("Starte Verbindungsaufbau (Versuch %d)...", startup_attempt)
            await connect_device()
            break
        except (TokenInvalidError, RuntimeError) as e:
            logging.error(
                "Startup endgültig fehlgeschlagen (auch Reauth über Home "
                "Assistant erfolglos, oder Geräte-Inkompatibilität): %s", e
            )
            publish_alert("critical", f"Daemon-Start endgültig fehlgeschlagen: {e}")
            return
        except Exception as e:
            wait = 30
            logging.error("Startup fehlgeschlagen: %s. Neuer Versuch in %ds...", e, wait)
            await asyncio.sleep(wait)

    try:
        logging.info("Lausche dauerhaft auf MQTT-Befehle unter '%s'...", mqtt_topic_cmd)

        health_task = asyncio.create_task(health_check_loop())

        try:
            await health_task
        except asyncio.CancelledError:
            logging.info("Hauptschleife wurde abgebrochen (Shutdown).")

    except Exception as e:
        # logging.exception() schreibt Message + vollständigen Stacktrace über
        # dieselben Handler wie der Rest des Logs (Datei + Konsole) - anders
        # als traceback.print_exc(), das nur nach stderr schreibt und bei
        # "nohup ... > /dev/null 2>&1" verloren geht.
        logging.exception("Schwerwiegender Fehler im Hauptprozess: %s", e)
    finally:
        logging.info("Fahre Dienste herunter...")
        if mqtt_client:
            try:
                mqtt_client.publish(availability_topic, "offline", retain=True)
                mqtt_client.publish(connectivity_topic, "OFF", retain=True)
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            except Exception:
                pass
        if manager:
            try:
                await manager.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Programm durch Benutzer beendet.")
