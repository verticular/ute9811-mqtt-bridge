# UTE9811+ Power Meter MQTT Bridge

This project bridges a **UNI-T UTE9811+ Power Meter** to an MQTT broker, enabling integration with **Home Assistant**. It reads voltage, current, power, and power factor data over a serial connection and publishes it as JSON payloads.

This script also supports **Home Assistant Auto-Discovery**, meaning entities will automatically appear in Home Assistant once connected.

## Features

- **Real-time Monitoring**: Reads Active Power, Power Factor, and Current.
- **Home Assistant Auto-Discovery**: Automatically creates sensors in HA.
- **Robust Serial Communication**: Handles connection errors and retires.

## Requirements

- Python 3.7+
- A UNI-T UTE9811+ Power Meter connected via USB/Serial.
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

## Configuration

You can configure the application using environment variables or by editing the default values in `main.py`.

| Variable | Default | Description |
|----------|---------|-------------|
| `SERIAL_PORT` | `/dev/ttyUSB0` | The serial device path for the meter. |
| `BAUD_RATE` | `115200` | Serial baud rate. |
| `MQTT_BROKER` | `192.168.0.100` | IP address or hostname of the MQTT broker. |
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
