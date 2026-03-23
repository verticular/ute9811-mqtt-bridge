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

import curses
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


# Seven-segment style big digits for the main power display (5 rows x 3 cols)
BIG_DIGITS = {
    '0': ['███', '█ █', '█ █', '█ █', '███'],
    '1': [' █ ', '██ ', ' █ ', ' █ ', '███'],
    '2': ['███', '  █', '███', '█  ', '███'],
    '3': ['███', '  █', '███', '  █', '███'],
    '4': ['█ █', '█ █', '███', '  █', '  █'],
    '5': ['███', '█  ', '███', '  █', '███'],
    '6': ['███', '█  ', '███', '█ █', '███'],
    '7': ['███', '  █', '  █', '  █', '  █'],
    '8': ['███', '█ █', '███', '█ █', '███'],
    '9': ['███', '█ █', '███', '  █', '███'],
    '.': ['   ', '   ', '   ', '   ', ' █ '],
    '-': ['   ', '   ', '███', '   ', '   '],
    ' ': ['   ', '   ', '   ', '   ', '   '],
    'W': ['█   █', '█   █', '█ █ █', '█████', ' █ █ '],
    '°': ['██ ', '██ ', '   ', '   ', '   '],
    'C': ['███', '█  ', '█  ', '█  ', '███']
}


class FrontPanelDisplay:
    """Curses-based CLI display resembling the UNI-T UTE9811+ front panel."""

    def __init__(self, bridge: 'PowerMeterBridge'):
        self.bridge = bridge

    def run(self):
        """Start the curses display."""
        curses.wrapper(self._main)

    def _main(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.timeout(500)

        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_GREEN, -1)    # LCD values
            curses.init_pair(2, curses.COLOR_CYAN, -1)     # Labels
            curses.init_pair(3, curses.COLOR_YELLOW, -1)   # Units
            curses.init_pair(4, curses.COLOR_WHITE, -1)    # Borders
            curses.init_pair(5, curses.COLOR_RED, -1)      # Brand accent
            curses.init_pair(6, curses.COLOR_MAGENTA, -1)  # OWON section

        while self.bridge.running:
            try:
                stdscr.erase()
                h, w = stdscr.getmaxyx()

                with self.bridge.state_lock:
                    ute = self.bridge.ute_state.copy()
                    owon = self.bridge.owon_state.copy()

                self._draw_panel(stdscr, h, w, ute, owon)
                stdscr.refresh()

                key = stdscr.getch()
                if key in (ord('q'), ord('Q'), 27):
                    self.bridge.running = False
                    break
                elif key in (ord('w'), ord('W')):
                    self.bridge.ute_delay = max(0.01, self.bridge.ute_delay - 0.05)
                elif key in (ord('s'), ord('S')):
                    self.bridge.ute_delay = min(5.0, self.bridge.ute_delay + 0.05)
                elif key in (ord('e'), ord('E')):
                    self.bridge.owon_delay = max(0.1, self.bridge.owon_delay - 0.1)
                elif key in (ord('d'), ord('D')):
                    self.bridge.owon_delay = min(10.0, self.bridge.owon_delay + 0.1)
            except curses.error:
                pass

    def _draw_panel(self, stdscr, h, w, ute, owon):
        if w < 44 or h < 15:
            msg = "Window too small to render the UI!"
            try:
                stdscr.addstr(h // 2, max(0, (w - len(msg)) // 2), msg, curses.color_pair(5) | curses.A_BOLD)
            except curses.error:
                pass
            return

        pw = max(45, w - 2)
        sx = max(0, (w - pw) // 2)
        inner = pw - 2
        row = 1

        # Header
        self._hline(stdscr, row, sx, '╔', '═', '╗', pw); row += 1
        self._bordered_text(stdscr, row, sx, pw, 'UNI-T  UTE9811+', center=True,
                            attr=curses.color_pair(5) | curses.A_BOLD); row += 1
        self._bordered_text(stdscr, row, sx, pw, 'DIGITAL POWER METER', center=True,
                            attr=curses.color_pair(2)); row += 1
        self._hline(stdscr, row, sx, '╠', '═', '╣', pw); row += 1

        # Main power display
        self._bordered_text(stdscr, row, sx, pw, "ACTIVE POWER", center=True, attr=curses.color_pair(4) | curses.A_BOLD); row += 1
        power_val = ute.get('power')
        power_str = f"{power_val:.2f}" if power_val is not None else "---"
        row = self._draw_big_number(stdscr, row, sx, pw, power_str, "W")
        self._bordered_empty(stdscr, row, sx, pw); row += 1

        # Secondary readings
        self._hline(stdscr, row, sx, '╠', '─', '╣', pw); row += 1

        readings = [
            ("VOLTAGE",      ute.get('voltage'),      "V",  "{:.2f}"),
            ("CURRENT",      ute.get('current'),      "A",  "{:.4f}"),
            ("FREQUENCY",    ute.get('frequency'),    "Hz", "{:.2f}"),
            ("POWER FACTOR", ute.get('power_factor'), "",   "{:.4f}"),
        ]

        for label, val, unit, fmt in readings:
            val_str = fmt.format(val) if val is not None else "---"
            self._draw_reading(stdscr, row, sx, pw, label, val_str, unit)
            row += 1

        if getattr(self.bridge, 'display', False) and pw >= 76 and any(k in ute for k in ('v_thd', 'c_thd', 'v_harm_rms', 'c_harm_rms', 'p_harm_rms')):
            self._hline(stdscr, row, sx, '╠', '─', '╣', pw); row += 1
            self._bordered_text(stdscr, row, sx, pw, '  HARMONICS INFO',
                                attr=curses.color_pair(3) | curses.A_BOLD); row += 1
            
            harmonics = [
                ("V THD",        ute.get('v_thd'),        "%",  "{:.2f}"),
                ("I THD",        ute.get('c_thd'),        "%",  "{:.2f}"),
                ("V HARM RMS",   ute.get('v_harm_rms'),   "V",  "{:.2f}"),
                ("I HARM RMS",   ute.get('c_harm_rms'),   "A",  "{:.4f}"),
                ("P HARM RMS",   ute.get('p_harm_rms'),   "W",  "{:.2f}"),
            ]
            for label, val, unit, fmt in harmonics:
                val_str = fmt.format(val) if val is not None else "---"
                self._draw_reading(stdscr, row, sx, pw, label, val_str, unit)
                row += 1

            if 'v_harm_arr' in ute:
                row = self._draw_histogram(stdscr, row, sx, pw, "V HARM %", ute['v_harm_arr'])
            if 'c_harm_arr' in ute:
                row = self._draw_histogram(stdscr, row, sx, pw, "I HARM %", ute['c_harm_arr'])
            if 'v_harm_arr' in ute or 'c_harm_arr' in ute:
                row = self._draw_histogram_legend(stdscr, row, sx, pw)

        # OWON section
        if self.bridge.use_owon:
            self._hline(stdscr, row, sx, '╠', '═', '╣', pw); row += 1
            self._bordered_text(stdscr, row, sx, pw, 'OWON  XDM2041', center=True,
                                attr=curses.color_pair(6) | curses.A_BOLD); row += 1
            self._bordered_text(stdscr, row, sx, pw, 'DIGITAL MULTIMETER', center=True,
                                attr=curses.color_pair(2)); row += 1
            self._hline(stdscr, row, sx, '╠', '═', '╣', pw); row += 1
            temp = owon.get('temperature')
            temp_str = f"{temp:.1f}" if temp is not None else "---"
            self._bordered_text(stdscr, row, sx, pw, "TEMPERATURE", center=True, attr=curses.color_pair(4) | curses.A_BOLD); row += 1
            row = self._draw_big_number(stdscr, row, sx, pw, temp_str, "°C")

        # Status bar
        self._hline(stdscr, row, sx, '╠', '═', '╣', pw); row += 1
        
        ports_str = f" UTE: {self.bridge.ute_port} | OWON: {self.bridge.owon_port if self.bridge.use_owon else 'N/A'} "
        self._bordered_text(stdscr, row, sx, pw, ports_str, attr=curses.color_pair(2)); row += 1
        
        ute_rate_str = f" UTE Rate: {self.bridge.ute_rate:.1f}Hz [w/s]"
        owon_rate_str = f" | OWON Rate: {self.bridge.owon_rate:.1f}Hz [e/d]" if self.bridge.use_owon else ""
        rates_str = ute_rate_str + owon_rate_str
        self._bordered_text(stdscr, row, sx, pw, rates_str, attr=curses.color_pair(2)); row += 1
        
        mqtt_ok = self.bridge.mqtt_client.is_connected()
        status_left = " MQTT: Connected" if mqtt_ok else " MQTT: Disconnected"
        status_right = "[q] Quit "
        gap = inner - len(status_left) - len(status_right)
        status_line = status_left + ' ' * max(1, gap) + status_right
        self._bordered_text(stdscr, row, sx, pw, status_line,
                            attr=curses.color_pair(2)); row += 1
        self._hline(stdscr, row, sx, '╚', '═', '╝', pw)

    # --- Drawing helpers ---

    def _hline(self, stdscr, y, x, left, fill, right, width):
        try:
            stdscr.addstr(y, x, left + fill * (width - 2) + right, curses.color_pair(4))
        except curses.error:
            pass

    def _bordered_empty(self, stdscr, y, x, pw):
        try:
            stdscr.addstr(y, x, '║' + ' ' * (pw - 2) + '║', curses.color_pair(4))
        except curses.error:
            pass

    def _bordered_text(self, stdscr, y, x, pw, text, center=False, attr=0):
        inner = pw - 2
        padded = text.center(inner) if center else text.ljust(inner)
        padded = padded[:inner]
        try:
            stdscr.addstr(y, x, '║', curses.color_pair(4))
            stdscr.addstr(y, x + 1, padded, attr)
            stdscr.addstr(y, x + pw - 1, '║', curses.color_pair(4))
        except curses.error:
            pass

    def _draw_big_number(self, stdscr, start_y, sx, pw, value_str, unit):
        """Render a value using big block digits and unit."""
        inner = pw - 2
        rows = [''] * 5
        for ch in value_str:
            glyph = BIG_DIGITS.get(ch, BIG_DIGITS[' '])
            for i in range(5):
                rows[i] += glyph[i] + ' '

        unit_rows = [''] * 5
        for ch in unit:
            glyph = BIG_DIGITS.get(ch, BIG_DIGITS[' '])
            for i in range(5):
                unit_rows[i] += glyph[i] + ' '

        text_w = len(rows[0])
        unit_w = len(unit_rows[0])
        space_between = 2 if unit else 0
        total_w = text_w + space_between + unit_w
        left_pad = max(0, (inner - total_w) // 2)

        for i in range(5):
            try:
                stdscr.addstr(start_y + i, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
                stdscr.addstr(start_y + i, sx + 1 + left_pad, rows[i], curses.color_pair(1) | curses.A_BOLD)
                if unit:
                    stdscr.addstr(start_y + i, sx + 1 + left_pad + text_w + space_between, unit_rows[i], curses.color_pair(3) | curses.A_BOLD)
            except curses.error:
                pass

        return start_y + 5

    def _draw_reading(self, stdscr, y, sx, pw, label, value, unit):
        """Draw a single reading line with colored segments."""
        inner = pw - 2
        try:
            stdscr.addstr(y, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
            stdscr.addstr(y, sx + 4, label, curses.color_pair(2))
            stdscr.addstr(y, sx + 21, '▸', curses.color_pair(4) | curses.A_DIM)
            stdscr.addstr(y, sx + 23, f"{value:>10s}",
                          curses.color_pair(1) | curses.A_BOLD)
            if unit:
                stdscr.addstr(y, sx + 34, unit, curses.color_pair(3))
        except curses.error:
            pass
            
    def _draw_histogram(self, stdscr, start_row, sx, pw, label, values):
        if not values:
            return start_row
            
        data = values[:15]
        BARS = [' ', '▂', '▃', '▄', '▅', '▆', '▇', '█']
        
        # Max ignoring the fundamental (index 0) if multiple values
        max_val = max(data[1:]) if len(data) > 1 and max(data[1:]) > 0 else 1.0
        inner = pw - 2
        
        try:
            # Row 1: Graph bars
            stdscr.addstr(start_row, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
            stdscr.addstr(start_row, sx + 2, f"{label[:8]:<8}", curses.color_pair(3) | curses.A_BOLD)
            
            x_offset_start = sx + 12
            spacing = max(4, (pw - 14) // 15)

            # Draw background grid dots first
            for i in range(len(data)):
                stdscr.addstr(start_row, x_offset_start + i * spacing + 1, '·', curses.color_pair(4) | curses.A_DIM)

            for i, v in enumerate(data):
                if i == 0:
                    idx = len(BARS) - 1 # full bar
                    color = curses.color_pair(2) | curses.A_BOLD # Cyan
                else:
                    ratio = min(max(v / max_val, 0.0), 1.0)
                    idx = int(ratio * (len(BARS) - 1))
                    if v >= 5.0:
                        color = curses.color_pair(5) | curses.A_BOLD # Red
                    elif v >= 2.0:
                        color = curses.color_pair(3) | curses.A_BOLD # Yellow
                    else:
                        color = curses.color_pair(1) | curses.A_BOLD # Green
                        
                # Only overwrite the grid dot if there's an actual bar block
                if BARS[idx] != ' ':
                    stdscr.addstr(start_row, x_offset_start + i * spacing + 1, BARS[idx], color)
            
            # Row 2: Percentage values
            row_2 = start_row + 1
            stdscr.addstr(row_2, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
            stdscr.addstr(row_2, sx + 2, f"{'val %':>8}", curses.color_pair(2))
            for i, v in enumerate(data):
                if i == 0:
                    color = curses.color_pair(2) | curses.A_BOLD
                else:
                    if v >= 5.0:
                        color = curses.color_pair(5) | curses.A_BOLD
                    elif v >= 2.0:
                        color = curses.color_pair(3) | curses.A_BOLD
                    else:
                        color = curses.color_pair(1) | curses.A_BOLD
                        
                if spacing >= 8:
                    s_val = f"{v:.2f}"
                    width = 6
                elif spacing >= 6:
                    s_val = f"{v:.1f}"
                    width = 5
                else:
                    s_val = f"{int(v)}"
                    width = 3
                    
                s = s_val.center(width)
                start_x = x_offset_start + i * spacing - (width // 2 - 1)
                stdscr.addstr(row_2, start_x, s, color)

            # Row 3: Harmonic indices
            row_3 = start_row + 2
            stdscr.addstr(row_3, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
            stdscr.addstr(row_3, sx + 2, f"{'har #':>8}", curses.color_pair(2))
            for i in range(len(data)):
                idx_str = str(i+1).center(3)
        except curses.error:
            pass

        return start_row + 3

    def _draw_histogram_legend(self, stdscr, row, sx, pw):
        inner = pw - 2
        try:
            stdscr.addstr(row, sx, '║' + ' ' * inner + '║', curses.color_pair(4))
            
            legend = "Distortion colors: "
            stdscr.addstr(row, sx + 2, legend, curses.color_pair(4))
            
            offset = sx + 2 + len(legend)
            
            stdscr.addstr(row, offset, "█", curses.color_pair(2) | curses.A_BOLD)
            stdscr.addstr(row, offset + 2, "Fund.", curses.color_pair(4))
            offset += 9
            
            stdscr.addstr(row, offset, "█", curses.color_pair(1) | curses.A_BOLD)
            stdscr.addstr(row, offset + 2, "< 2%", curses.color_pair(4))
            offset += 8
            
            stdscr.addstr(row, offset, "█", curses.color_pair(3) | curses.A_BOLD)
            stdscr.addstr(row, offset + 2, "2%-5%", curses.color_pair(4))
            offset += 9
            
            stdscr.addstr(row, offset, "█", curses.color_pair(5) | curses.A_BOLD)
            stdscr.addstr(row, offset + 2, "> 5%", curses.color_pair(4))
            
        except curses.error:
            pass

        return row + 1

class PowerMeterBridge:
    """Handles communication between the UTE9811+ meter and MQTT broker."""

    def __init__(self, use_owon: bool = False, auto_detect: bool = True, poll_extra: bool = True, display: bool = False):
        # Initialize MQTT Client
        # Note: CallbackAPIVersion.VERSION2 is required for paho-mqtt v2.0+
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.serial_conn: Optional[serial.Serial] = None
        self.use_owon = use_owon
        self.owon_conn: Optional[serial.Serial] = None
        self.poll_extra = poll_extra
        self.display = display
        
        self.ute_port = UTE_SERIAL_PORT
        self.owon_port = OWON_SERIAL_PORT
        
        self.ute_state: Dict[str, float] = {}
        self.owon_state: Dict[str, float] = {}
        self.state_lock = threading.Lock()
        self.running = False
        
        self.ute_delay = 0.1
        self.owon_delay = 1.0
        self.ute_timestamps: List[float] = []
        self.owon_timestamps: List[float] = []
        self.ute_rate = 0.0
        self.owon_rate = 0.0
        
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

        if getattr(self, 'display', False):
            sensors.extend([
                SensorConfig(
                    name="Voltage THD",
                    device_class="voltage",
                    unique_suffix="voltage_thd",
                    value_template="{{ value_json.v_thd }}",
                    state_topic=STATE_TOPIC,
                    unit_of_measurement="%"
                ),
                SensorConfig(
                    name="Current THD",
                    device_class="current",
                    unique_suffix="current_thd",
                    value_template="{{ value_json.c_thd }}",
                    state_topic=STATE_TOPIC,
                    unit_of_measurement="%"
                ),
                SensorConfig(
                    name="Voltage Harmonic RMS",
                    device_class="voltage",
                    unique_suffix="voltage_harm_rms",
                    value_template="{{ value_json.v_harm_rms }}",
                    state_topic=STATE_TOPIC,
                    unit_of_measurement="V"
                ),
                SensorConfig(
                    name="Current Harmonic RMS",
                    device_class="current",
                    unique_suffix="current_harm_rms",
                    value_template="{{ value_json.c_harm_rms }}",
                    state_topic=STATE_TOPIC,
                    unit_of_measurement="A"
                ),
                SensorConfig(
                    name="Power Harmonic RMS",
                    device_class="power",
                    unique_suffix="power_harm_rms",
                    value_template="{{ value_json.p_harm_rms }}",
                    state_topic=STATE_TOPIC,
                    unit_of_measurement="W"
                )
            ])

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
                v_thd, c_thd = None, None
                v_harm_rms, c_harm_rms, p_harm_rms = None, None, None
                v_harm_arr, c_harm_arr = None, None
                if self.poll_extra:
                    # Power Factor
                    pf = self._read_metric(b':MEASure:PFACtor?')
                    # Current
                    current = self._read_metric(b':MEASure:CURRent?')
                    # Voltage
                    voltage = self._read_metric(b':MEASure:VOLTage?')
                    # Frequency
                    frequency = self._read_metric(b':MEASure:FREQuency?')

                    if getattr(self, 'display', False):
                        v_thd = self._read_metric(b':MEASure:VOLTage:THD? PERCENT')
                        c_thd = self._read_metric(b':MEASure:CURRent:THD? PERCENT')
                        v_harm_rms = self._read_metric(b':MEASure:VOLTage:HARMonic:RMS?')
                        c_harm_rms = self._read_metric(b':MEASure:CURRent:HARMonic:RMS?')
                        p_harm_rms = self._read_metric(b':MEASure:POWer:HARMonic:RMS?')
                        v_harm_arr = self._read_metric_array(b':MEASure:VOLTage:HARMonic:ARRay? PERCENT')
                        c_harm_arr = self._read_metric_array(b':MEASure:CURRent:HARMonic:ARRay? PERCENT')

                with self.state_lock:
                    if power is not None: self.ute_state['power'] = power
                    if pf is not None: self.ute_state['power_factor'] = pf
                    if current is not None: self.ute_state['current'] = current
                    if voltage is not None: self.ute_state['voltage'] = voltage
                    if frequency is not None: self.ute_state['frequency'] = frequency
                    if v_thd is not None: self.ute_state['v_thd'] = v_thd
                    if c_thd is not None: self.ute_state['c_thd'] = c_thd
                    if v_harm_rms is not None: self.ute_state['v_harm_rms'] = v_harm_rms
                    if c_harm_rms is not None: self.ute_state['c_harm_rms'] = c_harm_rms
                    if p_harm_rms is not None: self.ute_state['p_harm_rms'] = p_harm_rms
                    if v_harm_arr: self.ute_state['v_harm_arr'] = v_harm_arr
                    if c_harm_arr: self.ute_state['c_harm_arr'] = c_harm_arr
                    
                now = time.time()
                self.ute_timestamps.append(now)
                if len(self.ute_timestamps) > 10:
                    self.ute_timestamps.pop(0)
                if len(self.ute_timestamps) >= 2:
                    dt = self.ute_timestamps[-1] - self.ute_timestamps[0]
                    self.ute_rate = (len(self.ute_timestamps) - 1) / dt if dt > 0 else 0.0
            except Exception as e:
                logger.error(f"Error communicating with UTE serial device: {e}")
            
            time.sleep(self.ute_delay)

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
                        
                    now = time.time()
                    self.owon_timestamps.append(now)
                    if len(self.owon_timestamps) > 10:
                        self.owon_timestamps.pop(0)
                    if len(self.owon_timestamps) >= 2:
                        dt = self.owon_timestamps[-1] - self.owon_timestamps[0]
                        self.owon_rate = (len(self.owon_timestamps) - 1) / dt if dt > 0 else 0.0
                except ValueError:
                    pass # Ignore unparseable values
            except Exception as e:
                logger.error(f"Error reading OWON: {e}")
            
            time.sleep(self.owon_delay)

    def run(self):
        """Main loop."""
        self.connect_serial()
        logger.info("Starting measurement loops...")
        self.running = True

        self.threads = []

        ute_thread = threading.Thread(target=self._ute_loop, daemon=True)
        ute_thread.start()
        self.threads.append(ute_thread)

        if self.use_owon:
            owon_thread = threading.Thread(target=self._owon_loop, daemon=True)
            owon_thread.start()
            self.threads.append(owon_thread)

        if self.display:
            publish_thread = threading.Thread(target=self._publish_loop, daemon=True)
            publish_thread.start()
            self.threads.append(publish_thread)
            self._run_display()
        else:
            self._run_headless()

    def _publish_loop(self):
        """Publishes measurement state to MQTT periodically."""
        while self.running:
            with self.state_lock:
                ute_copy = self.ute_state.copy()
                owon_copy = self.owon_state.copy()

            if ute_copy:
                self.mqtt_client.publish(STATE_TOPIC, json.dumps(ute_copy))
            if owon_copy:
                self.mqtt_client.publish(OWON_STATE_TOPIC, json.dumps(owon_copy))

            time.sleep(0.5)

    def _run_headless(self):
        """Run without display (original behavior)."""
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

                time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info("Stopping by user request...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.running = False
            self.cleanup()

    def _run_display(self):
        """Run with curses front panel display."""
        logging.getLogger().setLevel(logging.CRITICAL)
        panel = FrontPanelDisplay(self)
        try:
            panel.run()
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            self.cleanup()

    def cleanup(self):
        """Closes connections and waits for threads to exit."""
        self.running = False
        
        # Wait for threads to finish
        if hasattr(self, 'threads'):
            for t in self.threads:
                if t.is_alive():
                    t.join(timeout=2.0)
                    
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
    parser.add_argument("--owon", dest="owon", action="store_true", help="Enable OWON XDM2041 multimeter reading")
    parser.add_argument("--no-auto-detect", dest="auto_detect", action="store_false", help="Disable automatic detection of UTE and OWON serial ports (/dev/ttyUSB0, /dev/ttyUSB1)")
    parser.add_argument("--power-only", dest="poll_extra", action="store_false", help="Only poll active power on the UTE meter (disables pf, current, voltage, frequency)")
    parser.add_argument("--nodisplay", action="store_false", help="Show a live curses-based front panel display in the terminal")
    #parser.set_defaults(owon=True, auto_detect=True, poll_extra=False)
    
    args = parser.parse_args()

    bridge = PowerMeterBridge(use_owon=args.owon, auto_detect=args.auto_detect, poll_extra=args.poll_extra, display=args.nodisplay)
    bridge.run()


if __name__ == '__main__':
    main()
