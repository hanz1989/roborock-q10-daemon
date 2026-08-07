# Roborock Q10 – Room-by-Room Control via Google Home / Home Assistant

A 24/7 Raspberry Pi daemon that enables room-by-room control of a Roborock Q10 (Model `roborock.vacuum.ss07`, Protocol `B01`) via Google Home—completely bypassing the need for manual SSH interventions.

---

## Background

Unlike older Roborock models, the Q10 (B01 protocol, firmware `03.11.24`) lacks a local control API. Testing revealed the following:

* Pinging the local IP works, but ports `58867`, `8883`, and `1883` are closed (`ConnectionRefusedError`, verified via pure socket testing).
* The client library (`python-roborock`) marks `B01Q10Channel` as a pure MQTT/cloud wrapper; `is_local_connected` is hardcoded to `False`.

**Conclusion:** For this model and firmware, cloud control is not just a workaround—it is the only available architecture. This project implements a robust, persistently running cloud client instead of attempting local control.

---

## How It Works

* A Python daemon (`q10_fetch_map.py`) maintains an authenticated session with the Roborock Cloud API (token persistence prevents the need for repeated OTP logins).
* The daemon listens for MQTT commands (`roborock/buksi/command`) and triggers `clean_segments([room_id])` accordingly.
* Home Assistant and Google Home (via Matter) provide a switch for each room; an automation publishes the appropriate MQTT message upon activation.
* The daemon reports its status to `roborock/buksi/status` (retained).

---

## Robustness

* **Exponential backoff** for connection drops (5s / 30s / 120s).
* **Health check** every 15 minutes to repair connections as needed.
* **Clean interactive/headless split:** When running as a daemon (`systemd`), `input()` is never called—an invalid token triggers a retry rather than a crash.

---

## Prerequisites

* Raspberry Pi (or similar device) running Python 3.11+
* Roborock account with a linked device
* Local MQTT broker
* Home Assistant (for Google Home integration, optional)
* Libraries: `python-roborock`, `aiomqtt`, `pyyaml`

---

## Setup

```bash
git clone https://github.com/<your-username>/roborock-q10-daemon.git
cd roborock-q10-daemon
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp secrets.yaml.example secrets.yaml
# Fill in secrets.yaml with actual values (DUID, Local Key, MQTT Broker, etc.)

python q10_fetch_map.py   # Initial startup: interactive login (OTP via email)

```

Next, set it up as a `systemd` service:

```bash
sudo cp roborock-q10.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now roborock-q10.service

```

---

## Finding Room IDs

For this model, the Roborock Cloud API does not provide room data via `home.rooms`—room IDs are embedded within the map packages sent by the device:

```python
device.b01_q10_properties.map.rooms  # -> list[Q10Room] with id, raw_name, ...

```

---

## Status

* [x] Stable cloud control (login, reconnect, health check)
* [x] Production-ready daemon script
* [ ] Final `systemd` service installation on the Pi
* [ ] Google Home integration (Home Assistant automations)
* [ ] All room IDs verified (currently only the bathroom)

---

## License

MIT – see the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.

---

## Note

This project was developed with the assistance of an LLM (debugging, architecture, documentation). Feedback and pull requests are welcome, especially from other Q10 owners facing the same local API limitations.
