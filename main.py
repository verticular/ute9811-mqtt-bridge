#!/usr/bin/env python3
"""
UTE9811+ Power Meter MQTT Bridge

This script reads power measurements from a UNI-T UTE9811+ power meter over serial
and publishes the data to an MQTT broker, including Home Assistant auto-discovery configuration.

Requirements:
    - paho-mqtt
    - pyserial
    - python-dotenv

Usage:
    Adjust the configuration constants below or pass arguments via CLI (if implemented).
"""

import os
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import serial
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables from .env file (if exists)
load_dotenv()

# --- Configuration Constants ---

# Serial Configuration
SERIAL_PORT = os.getenv('SERIAL_PORT', '/dev/ttyUSB0')
BAUD_RATE = int(os.getenv('BAUD_RATE', 115200))
SERIAL_TIMEOUT = float(os.getenv('SERIAL_TIMEOUT', 1.0))

# MQTT Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER', '192.168.0.1')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASS = os.getenv('MQTT_PASS')
MQTT_KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', 60))

# Topic Configuration
STATE_TOPIC = os.getenv('MQTT_STATE_TOPIC', 'ute9811/state')
DISCOVERY_PREFIX = os.getenv('MQTT_DISCOVERY_PREFIX', 'homeassistant')
DEVICE_NAME = 'UTE9811+ Power Meter'
DEVICE_ID = 'ute9811_meter_01'
MANUFACTURER = 'UNI-T'
MODEL = 'UTE9811+'

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class SensorConfig:
    """Configuration structure for a single sensor entity."""
    name: str
    device_class: str
    unique_suffix: str
    value_template: str
    unit_of_measurement: Optional[str] = None
    state_class: str = "measurement"


class PowerMeterBridge:
    """Handles communication between the UTE9811+ meter and MQTT broker."""

    def __init__(self):
        # Initialize MQTT Client
        # Note: CallbackAPIVersion.VERSION2 is required for paho-mqtt v2.0+
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.serial_conn: Optional[serial.Serial] = None
        
        self._setup_mqtt()

    def _setup_mqtt(self):
        """Configures and connects the MQTT client."""
        if MQTT_USER and MQTT_PASS:
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
        
        self.mqtt_client.on_connect = self._on_connect
        
        try:
            logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
            self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
            self.mqtt_client.loop_start()
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            sys.exit(1)

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, rc: int, properties: Any = None):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            logger.info("Connected to MQTT Broker!")
            self.publish_discovery()
        else:
            logger.error(f"Failed to connect to MQTT, return code {rc}")

    def _get_device_info(self) -> Dict[str, Any]:
        """Returns the device information payload for Home Assistant."""
        return {
            "identifiers": [DEVICE_ID],
            "name": DEVICE_NAME,
            "manufacturer": MANUFACTURER,
            "model": MODEL
        }

    def publish_discovery(self):
        """Publishes MQTT Auto-Discovery payloads to Home Assistant."""
        logger.info("Publishing Home Assistant discovery messages...")
        
        sensors: List[SensorConfig] = [
            SensorConfig(
                name="Active Power",
                device_class="power",
                unique_suffix="power_w",
                value_template="{{ value_json.power }}",
                unit_of_measurement="W"
            ),
            SensorConfig(
                name="Power Factor",
                device_class="power_factor",
                unique_suffix="pf",
                value_template="{{ value_json.power_factor }}"
            ),
            SensorConfig(
                name="Current",
                device_class="current",
                unique_suffix="current_a",
                value_template="{{ value_json.current }}",
                unit_of_measurement="A"
            )
        ]

        device_info = self._get_device_info()
        availability_topic = f"{DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/status"

        for sensor in sensors:
            display_name = f"{DEVICE_NAME} {sensor.name}"
            unique_id = f"{DEVICE_ID}_{sensor.unique_suffix}"
            topic = f"{DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/{sensor.unique_suffix}/config"
            
            payload = {
                "name": sensor.name,  # HA appends this to device name usually, or use full name
                "state_topic": STATE_TOPIC,
                "value_template": sensor.value_template,
                "device_class": sensor.device_class,
                "state_class": sensor.state_class,
                "unique_id": unique_id,
                "device": device_info,
                "availability_topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline"
            }
            
            if sensor.unit_of_measurement:
                payload["unit_of_measurement"] = sensor.unit_of_measurement

            self.mqtt_client.publish(topic, json.dumps(payload), retain=True)

        # Publish initial availability
        self.mqtt_client.publish(availability_topic, "online", retain=True)
        logger.info("Discovery configuration published.")

    def connect_serial(self):
        """Establishes connection to the serial port."""
        try:
            logger.info(f"Opening serial port {SERIAL_PORT}...")
            self.serial_conn = serial.Serial(
                port=SERIAL_PORT,
                baudrate=BAUD_RATE,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=SERIAL_TIMEOUT
            )
            logger.info(f"Connected to serial port {SERIAL_PORT}")
        except serial.SerialException as e:
            logger.error(f"Could not open serial port {SERIAL_PORT}: {e}")
            sys.exit(1)

    def _read_metric(self, command: bytes) -> Optional[float]:
        """Sends a command to the serial port and parses the float response."""
        if not self.serial_conn:
            return None
            
        try:
            self.serial_conn.reset_input_buffer()
            self.serial_conn.write(command + b'\n')
            response = self.serial_conn.readline().decode('utf-8').strip()
            
            # Extract float using regex
            match = re.search(r"[-+]?\d*\.\d+|\d+", response)
            if match:
                return float(match.group())
            else:
                return None
        except Exception:
            return None

    def read_all_metrics(self) -> Optional[Dict[str, float]]:
        """Queries the meter for all metrics sequentially."""
        if not self.serial_conn or not self.serial_conn.is_open:
            return None

        try:
            # Query metrics sequentially
            # Note: Serial communication might require slight delays depending on the device responsiveness
            vals = {}
            
            # Power
            val = self._read_metric(b':MEASure:POWer:ACTive?')
            if val is None: return None
            vals['power'] = val
            
            # Power Factor
            val = self._read_metric(b':MEASure:PFACtor?')
            if val is None: return None
            vals['power_factor'] = val

            # Current
            val = self._read_metric(b':MEASure:CURRent?')
            if val is None: return None
            vals['current'] = val

            return vals

        except Exception as e:
            logger.error(f"Error communicating with serial device: {e}")
            return None

    def run(self):
        """Main loop."""
        self.connect_serial()
        logger.info("Starting measurement loop...")

        try:
            while True:
                metrics = self.read_all_metrics()
                
                if metrics:
                    payload = json.dumps(metrics)
                    self.mqtt_client.publish(STATE_TOPIC, payload)
                    logger.debug(f"Published: {payload}")
                else:
                    logger.debug("Failed to read complete metrics, retrying...")

                time.sleep(0.1)  # Adjust poll rate as needed

        except KeyboardInterrupt:
            logger.info("Stopping by user request...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Closes connections."""
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            logger.info("Serial connection closed.")
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            logger.info("MQTT connection closed.")


def main():
    bridge = PowerMeterBridge()
    bridge.run()


if __name__ == '__main__':
    main()
