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
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import serial
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables from .env file (if exists)
load_dotenv()

# --- Configuration Constants ---

# UTE9811+ Serial Configuration
UTE_SERIAL_PORT = os.getenv('UTE_SERIAL_PORT', '/dev/ttyUSB0')
UTE_BAUD_RATE = int(os.getenv('UTE_BAUD_RATE', 115200))
UTE_SERIAL_TIMEOUT = float(os.getenv('UTE_SERIAL_TIMEOUT', 1.0))

# OWON XDM2041 Serial Configuration
OWON_SERIAL_PORT = os.getenv('OWON_SERIAL_PORT', '/dev/ttyUSB1')
OWON_BAUD_RATE = int(os.getenv('OWON_BAUD_RATE', 115200))
OWON_SERIAL_TIMEOUT = float(os.getenv('OWON_SERIAL_TIMEOUT', 1.0))

# MQTT Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER', '192.168.0.1')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASS = os.getenv('MQTT_PASS')
MQTT_KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', 60))

# Topic Configuration
STATE_TOPIC = os.getenv('MQTT_STATE_TOPIC', 'ute9811/state')
OWON_STATE_TOPIC = os.getenv('OWON_MQTT_STATE_TOPIC', 'owon/state')
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
    state_topic: str
    unit_of_measurement: Optional[str] = None
    state_class: str = "measurement"
    device_id: str = DEVICE_ID
    device_name: str = DEVICE_NAME
    device_manufacturer: str = MANUFACTURER
    device_model: str = MODEL


class PowerMeterBridge:
    """Handles communication between the UTE9811+ meter and MQTT broker."""

    def __init__(self, use_owon: bool = False, auto_detect: bool = True, poll_extra: bool = True):
        # Initialize MQTT Client
        # Note: CallbackAPIVersion.VERSION2 is required for paho-mqtt v2.0+
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.serial_conn: Optional[serial.Serial] = None
        self.use_owon = use_owon
        self.owon_conn: Optional[serial.Serial] = None
        self.poll_extra = poll_extra
        
        self.ute_port = UTE_SERIAL_PORT
        self.owon_port = OWON_SERIAL_PORT
        
        self.ute_state: Dict[str, float] = {}
        self.owon_state: Dict[str, float] = {}
        self.state_lock = threading.Lock()
        self.running = False
        
        if auto_detect:
            self._autodetect_ports()
        
        self._setup_mqtt()

    def _autodetect_ports(self):
        """Probes /dev/ttyUSB0 and /dev/ttyUSB1 to find UTE and OWON."""
        ports_to_check = ['/dev/ttyUSB0', '/dev/ttyUSB1']
        logger.info("Auto-detecting serial ports...")
        
        found_ute = False
        found_owon = False
        
        for port in ports_to_check:
            try:
                # Open port with short timeout
                with serial.Serial(port, UTE_BAUD_RATE, timeout=0.5) as s:
                    # Try UTE9811+
                    if not found_ute:
                        s.reset_input_buffer()
                        s.write(b':MEASure:POWer:ACTive?\n')
                        time.sleep(0.1)
                        resp = s.readline().decode('utf-8', errors='ignore').strip()
                        try:
                            _ = float(resp)
                            logger.info(f"Auto-detected UTE9811+ on {port}")
                            self.ute_port = port
                            found_ute = True
                            continue
                        except ValueError:
                            pass
                    
                    # Try OWON
                    if self.use_owon and not found_owon:
                        s.reset_input_buffer()
                        s.write(b'CONF:TEMP\n')
                        time.sleep(0.5)
                        s.write(b'MEAS:TEMP?\n')
                        time.sleep(0.1)
                        resp = s.readline().decode('utf-8', errors='ignore').strip()
                        try:
                            _ = float(resp)
                            logger.info(f"Auto-detected OWON XDM2041 on {port}")
                            self.owon_port = port
                            found_owon = True
                            continue
                        except ValueError:
                            pass
            except Exception as e:
                logger.debug(f"Failed to probe {port}: {e}")

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

    def publish_discovery(self):
        """Publishes MQTT Auto-Discovery payloads to Home Assistant."""
        logger.info("Publishing Home Assistant discovery messages...")
        
        sensors: List[SensorConfig] = [
            SensorConfig(
                name="Active Power",
                device_class="power",
                unique_suffix="power_w",
                value_template="{{ value_json.power }}",
                state_topic=STATE_TOPIC,
                unit_of_measurement="W"
            ),
            SensorConfig(
                name="Power Factor",
                device_class="power_factor",
                unique_suffix="pf",
                value_template="{{ value_json.power_factor }}",
                state_topic=STATE_TOPIC
            ),
            SensorConfig(
                name="Current",
                device_class="current",
                unique_suffix="current_a",
                value_template="{{ value_json.current }}",
                state_topic=STATE_TOPIC,
                unit_of_measurement="A"
            ),
            SensorConfig(
                name="Voltage",
                device_class="voltage",
                unique_suffix="voltage_v",
                value_template="{{ value_json.voltage }}",
                state_topic=STATE_TOPIC,
                unit_of_measurement="V"
            ),
            SensorConfig(
                name="Frequency",
                device_class="frequency",
                unique_suffix="frequency_hz",
                value_template="{{ value_json.frequency }}",
                state_topic=STATE_TOPIC,
                unit_of_measurement="Hz"
            )
        ]

        if self.use_owon:
            sensors.append(SensorConfig(
                name="Temperature",
                device_class="temperature",
                unique_suffix="temperature_c",
                value_template="{{ value_json.temperature }}",
                state_topic=OWON_STATE_TOPIC,
                unit_of_measurement="°C",
                device_id="owon_xdm2041_01",
                device_name="OWON XDM2041",
                device_manufacturer="OWON",
                device_model="XDM2041"
            ))

        for sensor in sensors:
            display_name = f"{sensor.device_name} {sensor.name}"
            unique_id = f"{sensor.device_id}_{sensor.unique_suffix}"
            topic = f"{DISCOVERY_PREFIX}/sensor/{sensor.device_id}/{sensor.unique_suffix}/config"
            availability_topic = f"{DISCOVERY_PREFIX}/sensor/{sensor.device_id}/status"
            
            device_info = {
                "identifiers": [sensor.device_id],
                "name": sensor.device_name,
                "manufacturer": sensor.device_manufacturer,
                "model": sensor.device_model
            }
            
            payload = {
                "name": sensor.name,  # HA appends this to device name usually, or use full name
                "state_topic": sensor.state_topic,
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

            # Publish initial availability for the sensor's device
            self.mqtt_client.publish(availability_topic, "online", retain=True)
            
        logger.info("Discovery configuration published.")

    def connect_serial(self):
        """Establishes connection to the serial port."""
        try:
            logger.info(f"Opening UTE9811+ serial port {self.ute_port}...")
            self.serial_conn = serial.Serial(
                port=self.ute_port,
                baudrate=UTE_BAUD_RATE,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=UTE_SERIAL_TIMEOUT
            )
            logger.info(f"Connected to UTE9811+ serial port {self.ute_port}")
        except serial.SerialException as e:
            logger.error(f"Could not open UTE9811+ serial port {self.ute_port}: {e}")
            sys.exit(1)
            
        if self.use_owon:
            if self.ute_port == self.owon_port:
                logger.error(f"OWON and UTE ports are both set to {self.ute_port}! Disabling OWON to prevent data overriding and thread race conditions.")
                self.use_owon = False
                return
                
            try:
                logger.info(f"Opening OWON serial port {self.owon_port}...")
                self.owon_conn = serial.Serial(
                    port=self.owon_port,
                    baudrate=OWON_BAUD_RATE,
                    bytesize=serial.EIGHTBITS,
                    parity=serial.PARITY_NONE,
                    stopbits=serial.STOPBITS_ONE,
                    timeout=OWON_SERIAL_TIMEOUT
                )
                logger.info(f"Connected to OWON serial port {self.owon_port}")
                
                # Ensure the OWON measures temperature right after booting
                self.owon_conn.write(b'CONF:TEMP\n')
                time.sleep(0.5)
            except serial.SerialException as e:
                logger.error(f"Could not open OWON serial port {self.owon_port}: {e}")
                sys.exit(1)

    def _read_metric(self, command: bytes) -> Optional[float]:
        """Sends a command to the serial port and parses the float response."""
        if not self.serial_conn:
            return None
            
        try:
            self.serial_conn.reset_input_buffer()
            self.serial_conn.write(command + b'\n')
            response = self.serial_conn.readline().decode('utf-8').strip()
            
            # Extract float
            return float(response)
        except Exception:
            return None

    def _read_metric_array(self, command: bytes) -> List[float]:
        """Sends a command and expects a comma-separated list of floats."""
        if not self.serial_conn:
            return []
            
        try:
            self.serial_conn.reset_input_buffer()
            self.serial_conn.write(command + b'\n')
            response = self.serial_conn.readline().decode('utf-8').strip()
            
            # Split by comma and convert to floats
            parts = response.split(',')
            results = []
            for p in parts:
                try:
                    # Clean up string
                    clean_p = p.strip()
                    if clean_p:
                        results.append(float(clean_p))
                except ValueError:
                    continue
            return results
        except Exception:
            return []

    def _ute_loop(self):
        """Continuously reads metrics from the UTE meter."""
        while self.running:
            if not self.serial_conn or not self.serial_conn.is_open:
                time.sleep(1)
                continue

            try:
                # Power
                power = self._read_metric(b':MEASure:POWer:ACTive?')
                
                pf, current, voltage, frequency = None, None, None, None
                if self.poll_extra:
                    # Power Factor
                    pf = self._read_metric(b':MEASure:PFACtor?')
                    # Current
                    current = self._read_metric(b':MEASure:CURRent?')
                    # Voltage
                    voltage = self._read_metric(b':MEASure:VOLTage?')
                    # Frequency
                    frequency = self._read_metric(b':MEASure:FREQuency?')

                with self.state_lock:
                    if power is not None: self.ute_state['power'] = power
                    if pf is not None: self.ute_state['power_factor'] = pf
                    if current is not None: self.ute_state['current'] = current
                    if voltage is not None: self.ute_state['voltage'] = voltage
                    if frequency is not None: self.ute_state['frequency'] = frequency
            except Exception as e:
                logger.error(f"Error communicating with UTE serial device: {e}")
            
            time.sleep(0.1)

    def _owon_loop(self):
        """Continuously reads metrics from the OWON multimeter."""
        while self.running:
            if not self.owon_conn or not self.owon_conn.is_open:
                time.sleep(1)
                continue
            
            try:
                self.owon_conn.reset_input_buffer()
                self.owon_conn.write(b'MEAS:TEMP?\n')
                response = self.owon_conn.readline().decode('utf-8').strip()
                try:
                    val = float(response)
                    with self.state_lock:
                        self.owon_state['temperature'] = val
                except ValueError:
                    pass # Ignore unparseable values
            except Exception as e:
                logger.error(f"Error reading OWON: {e}")
            
            time.sleep(1.0)  # Slower poll rate for temperature/multimeter

    def run(self):
        """Main loop."""
        self.connect_serial()
        logger.info("Starting measurement loops...")
        self.running = True

        ute_thread = threading.Thread(target=self._ute_loop, daemon=True)
        ute_thread.start()

        if self.use_owon:
            owon_thread = threading.Thread(target=self._owon_loop, daemon=True)
            owon_thread.start()

        try:
            while True:
                with self.state_lock:
                    ute_copy = self.ute_state.copy()
                    owon_copy = self.owon_state.copy()
                
                if ute_copy:
                    payload = json.dumps(ute_copy)
                    self.mqtt_client.publish(STATE_TOPIC, payload)
                    logger.debug(f"UTE Published: {payload}")
                
                if owon_copy:
                    payload = json.dumps(owon_copy)
                    self.mqtt_client.publish(OWON_STATE_TOPIC, payload)
                    logger.debug(f"OWON Published: {payload}")
                    
                if not ute_copy and not owon_copy:
                    logger.debug("No metrics available yet...")

                time.sleep(0.5)  # MQTT publish rate

        except KeyboardInterrupt:
            logger.info("Stopping by user request...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.running = False
            self.cleanup()

    def cleanup(self):
        """Closes connections."""
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            logger.info("Serial connection closed.")
            
        if self.owon_conn and self.owon_conn.is_open:
            self.owon_conn.close()
            logger.info("OWON connection closed.")
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            logger.info("MQTT connection closed.")


import argparse

def main():
    parser = argparse.ArgumentParser(description="UTE9811+ Power Meter and OWON XDM2041 MQTT Bridge")
    parser.add_argument("--no-owon", dest="owon", action="store_false", help="Disable OWON XDM2041 multimeter reading")
    parser.add_argument("--no-auto-detect", dest="auto_detect", action="store_false", help="Disable automatic detection of UTE and OWON serial ports (/dev/ttyUSB0, /dev/ttyUSB1)")
    parser.add_argument("--power-only", dest="poll_extra", action="store_false", help="Only poll active power on the UTE meter (disables pf, current, voltage, frequency)")
    parser.set_defaults(owon=True, auto_detect=True, poll_extra=True)
    
    args = parser.parse_args()

    bridge = PowerMeterBridge(use_owon=args.owon, auto_detect=args.auto_detect, poll_extra=args.poll_extra)
    bridge.run()


if __name__ == '__main__':
    main()
