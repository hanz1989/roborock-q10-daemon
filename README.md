# Roborock Q10 – Room-by-Room Control via Google Home / Home Assistant

A 24/7 Raspberry Pi daemon that enables room-by-room control of a Roborock Q10 (Model `roborock.vacuum.ss07`, Protocol `B01`) via Google Home — completely bypassing the need for manual SSH interventions.

This README is written to be followable even if you've never run a Python script or set up a Raspberry Pi service before. Take it step by step — you don't need to understand every line of code to get this running.

---

## ⚠️ Before you start: is your vacuum actually supported?

This project was built and tested **specifically for the Roborock Q10** (model string `roborock.vacuum.ss07`, protocol `B01`). If your vacuum is a **Roborock Qrevo** (or another newer model), **this script will most likely NOT work as-is** — Qrevo devices generally speak a different, newer protocol (`V1`) that has a completely different internal structure. Running this daemon against a Qrevo will fail at startup with a clear error message (it won't silently do the wrong thing), but it won't clean anything either.

**If you're not 100% sure what you have:** run the included `check_device_model.py` script first (see below). It logs in, asks the Roborock cloud what your device actually is, prints the answer, and does **not** touch your vacuum in any way — no cleaning, no settings changed. Takes about 30 seconds.

```bash
python3 check_device_model.py
```

It will tell you plainly: "this looks like a Q10, this script should work" or "this looks like a Qrevo/V1 device, this script will not work for you." If you get the second result, please open a GitHub issue — a V1-compatible version of this daemon is a realistic follow-up project, just not what's in this repository yet.

---

## Background

Unlike older Roborock models, the Q10 (B01 protocol, firmware `03.11.24`) lacks a local control API. Testing revealed the following:

* Pinging the local IP works, but ports `58867`, `8883`, and `1883` are closed (`ConnectionRefusedError`, verified via pure socket testing).
* The client library (`python-roborock`) marks `B01Q10Channel` as a pure MQTT/cloud wrapper — there is no local fallback for this specific model family.

**Conclusion:** For this model and firmware, cloud control is not just a workaround — it is the only available architecture. This project implements a robust, persistently running cloud client instead of attempting local control.

---

## How It Works (in plain terms)

1. A Python program (`q10_fetch_map.py`) runs forever in the background on your Raspberry Pi. It logs into your Roborock cloud account once and keeps that login "session" alive, so it never has to bother you for a code again (unless Roborock invalidates it — see the Reauth section below).
2. It listens for simple text messages on a local MQTT topic (think of MQTT as a very lightweight messaging system your smart-home devices use to talk to each other). When it sees a message like `9`, it tells the vacuum "go clean room 9."
3. Home Assistant (and through it, Google Home) shows you a switch for each room. Flip the switch, and an automation sends the right MQTT message.
4. The daemon reports back what it's doing (`roborock/buksi/status`) so you can see progress in Home Assistant.

You don't need to understand MQTT or Python to *use* the finished setup — you'll just see room switches in the Google Home app. The complexity below is for the one-time setup.

---

## What You Need Before Starting

* A Raspberry Pi (or any always-on Linux machine) with Python 3.11 or newer
* A Roborock account with your vacuum already set up and working in the official Roborock app
* A local MQTT broker (this is software, not hardware — Home Assistant's built-in "Mosquitto" add-on is the easiest option if you don't have one)
* Home Assistant (only needed if you want the Google Home integration — the daemon itself works without it)
* A little bit of patience for the first-time login step (explained below)

---

## Setup, Step by Step

### 1. Get the code onto your Raspberry Pi

```bash
git clone https://github.com/hanz1989/roborock-q10-daemon.git
cd roborock-q10-daemon
```

### 2. Create a Python virtual environment (keeps this project's dependencies separate from everything else on your Pi)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Check that your vacuum is actually compatible

Skip ahead to step 4 first to create `secrets.yaml`, then come back here.

```bash
python3 check_device_model.py
```

Read its output carefully before continuing — see the warning section at the top of this README.

### 4. Fill in your configuration

```bash
cp secrets.yaml.example secrets.yaml
```

Open `secrets.yaml` in any text editor (`nano secrets.yaml` works fine on the Pi) and fill in:
- your Roborock account email
- your vacuum's DUID (a unique ID — `check_device_model.py` will show you this)
- your MQTT broker's address

The `cleaning:` section is optional and explained further down.

### 5. First-time login (only done once, ever)

```bash
python q10_fetch_map.py
```

The very first time you run this, it needs to log in interactively — Roborock will email you a one-time code, and the script will ask you to type it in. After this succeeds, it saves a `roborock_token.json` file, and you'll never need to do this again (the daemon takes over from here and re-logs-in automatically in the background if needed).

Once you see it connect successfully, stop it with `Ctrl+C` — we'll run it properly as a background service next.

### 6. Set it up to run forever in the background

```bash
sudo cp roborock-q10.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now roborock-q10.service
```

This tells your Raspberry Pi: "always run this program, and if it ever crashes or the Pi reboots, start it again automatically." You can check it's running with:

```bash
sudo systemctl status roborock-q10.service
```

---

## Finding Room IDs

For this model, the Roborock Cloud API does not provide room data via `home.rooms` — room IDs are embedded within the map data sent by the device instead:

```python
device.b01_q10_properties.map.rooms  # -> list[Q10Room] with id, raw_name, ...
```

If you're not comfortable poking around in Python to find these, the simplest approach is trial and error: send a room ID via MQTT (see below), watch which room the vacuum actually cleans, and note it down.

To send a command manually (useful for testing, and for finding room IDs):

```bash
mosquitto_pub -h <your-mqtt-broker> -t "roborock/buksi/command" -m "9"
```

**You can also clean multiple rooms in one go** — send a comma-separated list instead of a single number:

```bash
mosquitto_pub -h <your-mqtt-broker> -t "roborock/buksi/command" -m "9,3,5"
```

---

## About the current cleaning settings (mode, suction, and "double-pass")

The daemon currently sets the vacuum's cleaning mode and suction power **every time it starts a cleaning run**, based on `secrets.yaml`:

```yaml
cleaning:
  clean_mode: "VACUUM"    # VACUUM, MOP, or VAC_AND_MOP
  fan_level: "TURBO"      # OFF, QUIET, BALANCED, TURBO, MAX, or MAX_PLUS
  clean_line: null        # see below — experimental
```

By default this matches the original behavior: vacuum-only, at the "Turbo" (intensive) suction level. If it feels like your vacuum is currently always doing an intensive, "double-pass"-feeling clean, this is almost certainly why — `TURBO` is a strong setting. You can lower it (e.g. to `BALANCED` or `QUIET`) by editing `secrets.yaml`; no code changes needed.

There is a separate setting in the Roborock app for cleaning *route density* (roughly: how close together the vacuum's back-and-forth lines are — a finer/denser pattern will look like more thorough, "doubled-up" coverage of the same area). This project can attempt to control that too via the `clean_line` option (`FAST`, `DAILY`, or `FINE`), but **this has not been tested against real hardware yet** — it's included because the underlying protocol field exists, not because it's confirmed to work. If you try it, watch the first cleaning run to make sure it behaves as expected, and please report back (via a GitHub issue) whether it worked so this note can be updated.

---

## Robustness

* **Exponential backoff** for connection drops (5s / 30s / 120s).
* **Health check** every 15 minutes to detect and repair dead connections.
* **Clean interactive/headless split:** when running as a `systemd` service, `input()` is never called — an invalid token triggers an automatic reauthorization flow through Home Assistant instead of crashing (see below), or a retry if it's just a network hiccup.
* **Automatic reauthorization without SSH:** if your login token ever expires, the daemon requests a new one and waits for you to enter the code Roborock emails you into a Home Assistant helper — no need to SSH into the Pi.

---

## Status

* [x] Stable cloud control (login, reconnect, health check)
* [x] Production-ready daemon script for Q10 (B01 protocol)
* [x] Multi-room cleaning in a single command
* [x] Configurable cleaning mode / suction level via `secrets.yaml`
* [ ] Experimental `clean_line` (route density) setting — needs live verification
* [ ] Final `systemd` service installation on the Pi
* [ ] Google Home integration (Home Assistant automations)
* [ ] All room IDs verified (currently only the bathroom)
* [ ] Qrevo / V1-protocol support (separate effort — not started)

---

## License

MIT – see the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.

---

## Note on how this was built

This project was developed with the assistance of several different LLMs (large language models) across iterations — used for debugging, architecture decisions, documentation, and direct introspection of the `python-roborock` library's source code. This is disclosed openly and intentionally. If you're reviewing this code for your own use, especially the authentication/reauth logic and the experimental `clean_line` setting, treat it the way you'd treat any community script: read it, understand what it does before running it, and open an issue if something looks off.

Feedback and pull requests are welcome, especially from other Q10 owners — and especially from anyone with a Qrevo willing to help extend this to the V1 protocol.
