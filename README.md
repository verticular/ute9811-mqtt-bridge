# UTE9811+ Power Meter & OWON XDM2041 MQTT Bridge

This project bridges a **UNI-T UTE9811+ Power Meter** and an **OWON XDM2041 Multimeter** to an MQTT broker, enabling integration with **Home Assistant**. It reads voltage, current, power, and power factor data from the UTE9811+, and temperature data from the OWON XDM2041 over serial connections, publishing it as JSON payloads.

This script also supports **Home Assistant Auto-Discovery**, meaning entities will automatically appear in Home Assistant once connected.

## Features

- **Real-time Monitoring**: Reads Active Power, Power Factor, Current, Voltage, and Temperature.
- **Home Assistant Auto-Discovery**: Automatically creates sensors in HA.
- **Auto-Detection**: Automatically detects the serial ports (`/dev/ttyUSB0` and `/dev/ttyUSB1`) for the UTE9811+ and OWON multimeter by default.
- **Robust Serial Communication**: Handles connection errors and retries.

## Requirements

- Python 3.7+
- A UNI-T UTE9811+ Power Meter connected via USB/Serial.
- An OWON XDM2041 Multimeter connected via USB/Serial (optional but enabled by default).
- An MQTT Broker (e.g., Mosquitto, Home Assistant Add-on).

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/verticular/ute9811-mqtt-bridge.git
   cd ute9811-mqtt-bridge
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Command Line Options

```bash
python main.py [OPTIONS]
```
| Flag | Description |
|------|-------------|
| `--owon` | Enable reading temperature from the OWON XDM2041 multimeter. (OWON connectivity is disabled by default) |
| `--no-auto-detect`| Disable automatic detection of UTE and OWON serial ports. If set, explicit environment variable ports will be used. |
| `--power-only` | Only poll active power on the UTE meter (disables pf, current, voltage, frequency) |
| `--nodisplay` | Disable live curses-based front panel display in the terminal |

### Display Mode Keyboard Controls

When running with the `--display` flag, the bottom status bar will show the active serial ports and the real-time sampling rates (measurements per second). You can dynamically adjust the reading delays using the following keys:

- **`w`** / **`s`**: Increase / Decrease UTE9811+ sampling rate.
- **`e`** / **`d`**: Increase / Decrease OWON XDM2041 sampling rate.
- **`q`**: Quit the application.

## Configuration

You can configure the application using environment variables, an `.env` file, or by passing the command-line options.

| Variable | Default | Description |
|----------|---------|-------------|
| `UTE_SERIAL_PORT` | `/dev/ttyUSB0` | The serial device path for the UTE9811+ meter (if auto-detect is disabled). |
| `UTE_BAUD_RATE` | `115200` | Serial baud rate for the UTE9811+. |
| `OWON_SERIAL_PORT`| `/dev/ttyUSB1` | The serial device path for the OWON multimeter (if auto-detect is disabled). |
| `OWON_BAUD_RATE` | `115200` | Serial baud rate for the OWON multimeter. |
| `MQTT_BROKER` | `192.168.0.1` | IP address or hostname of the MQTT broker. |
| `MQTT_PORT` | `1883` | MQTT port. |
| `MQTT_USER` | `myuser` | MQTT username (optional). |
| `MQTT_PASS` | `mypassword` | MQTT password (optional). |
| `MQTT_STATE_TOPIC` | `ute9811/state` | Topic for publishing sensor data. |

### Running with Environment Variables

```bash
export MQTT_BROKER="192.168.0.100"
export MQTT_USER="myuser"
export MQTT_PASS="mypassword"
python main.py
```

## License

MIT License
