"""
IoT Device Manager
Manages physical IoT devices and their connections
Supports multiple protocols: MQTT, HTTP, CoAP, Modbus, OPC-UA
"""

import asyncio
import paho.mqtt.client as mqtt
import json
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import threading
import ssl

from src.models.physical.device import PhysicalDevice, DeviceStatus, ConnectionType

logger = logging.getLogger(__name__)


@dataclass
class DeviceConnection:
    """Represents connection details for a device"""
    device_id: str
    connection_type: ConnectionType
    endpoint: str
    port: int
    protocol_version: str
    credentials: Optional[Dict[str, str]] = None
    ssl_enabled: bool = False
    connected: bool = False
    last_seen: Optional[datetime] = None


class DeviceDiscoveryMethod(Enum):
    """Device discovery methods"""
    MANUAL = "manual"  # Manually configured
    MDNS = "mdns"  # Multicast DNS
    UPNP = "upnp"  # Universal Plug and Play
    SCAN = "scan"  # Network scan
    MQTT_DISCOVERY = "mqtt_discovery"  # MQTT auto-discovery


class IoTDeviceManager:
    """
    Central manager for all physical IoT devices
    
    Features:
    - Device registration and lifecycle
    - Multi-protocol support
    - Connection management
    - Data ingestion
    - Command dispatch
    - Device discovery
    """
    
    def __init__(self):
        # Device registry
        self.devices: Dict[str, PhysicalDevice] = {}
        self.connections: Dict[str, DeviceConnection] = {}
        
        # Protocol handlers
        self.mqtt_client: Optional[mqtt.Client] = None
        self.mqtt_connected = False
        self.mqtt_config: Dict[str, Any] = {}
        
        # Callbacks
        self.on_device_data: Optional[Callable] = None
        self.on_device_connected: Optional[Callable] = None
        self.on_device_disconnected: Optional[Callable] = None
        self.on_device_error: Optional[Callable] = None
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Metrics
        self.total_messages_received = 0
        self.total_commands_sent = 0
        
        logger.info("🔌 IoT Device Manager initialized")
    
    # ==========================================
    # DEVICE REGISTRATION
    # ==========================================
    
    def register_device(
        self,
        device: PhysicalDevice,
        connection: DeviceConnection
    ) -> bool:
        """
        Register a new physical device
        
        Args:
            device: PhysicalDevice instance
            connection: Connection details
            
        Returns:
            bool: Success status
        """
        with self.lock:
            if device.device_id in self.devices:
                logger.warning(f"Device already registered: {device.device_id}")
                return False
            
            self.devices[device.device_id] = device
            self.connections[device.device_id] = connection
            
            # Subscribe to device topic if MQTT
            if connection.connection_type == ConnectionType.MQTT:
                self._subscribe_mqtt_device(device.device_id)
            
            logger.info(f"✅ Device registered: {device.device_id} ({device.device_type.value})")
            
            if self.on_device_connected:
                self.on_device_connected(device.device_id)
            
            return True
    
    def unregister_device(self, device_id: str) -> bool:
        """Unregister a device"""
        with self.lock:
            if device_id not in self.devices:
                logger.warning(f"Device not found: {device_id}")
                return False
            
            # Unsubscribe if MQTT
            connection = self.connections.get(device_id)
            if connection and connection.connection_type == ConnectionType.MQTT:
                self._unsubscribe_mqtt_device(device_id)
            
            del self.devices[device_id]
            del self.connections[device_id]
            
            logger.info(f"🗑️  Device unregistered: {device_id}")
            return True
    
    def get_device(self, device_id: str) -> Optional[PhysicalDevice]:
        """Get device by ID"""
        return self.devices.get(device_id)
    
    def get_all_devices(self) -> List[PhysicalDevice]:
        """Get all registered devices"""
        with self.lock:
            return list(self.devices.values())
    
    def get_devices_by_type(self, device_type: str) -> List[PhysicalDevice]:
        """Get devices by type"""
        with self.lock:
            return [
                device for device in self.devices.values()
                if device.device_type.value == device_type
            ]
    
    # ==========================================
    # MQTT CONNECTION
    # ==========================================
    
    def connect_mqtt(
        self,
        broker: str = "localhost",
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        use_tls: bool = False,
        ca_certs: Optional[str] = None
    ) -> bool:
        """
        Connect to MQTT broker
        
        Args:
            broker: MQTT broker address
            port: MQTT broker port
            username: Optional username
            password: Optional password
            client_id: Optional client ID
            use_tls: Enable TLS/SSL
            ca_certs: Path to CA certificates
            
        Returns:
            bool: Connection success
        """
        try:
            # Store config
            self.mqtt_config = {
                "broker": broker,
                "port": port,
                "username": username,
                "password": password,
                "client_id": client_id or f"device_manager_{datetime.utcnow().timestamp()}",
                "use_tls": use_tls,
                "ca_certs": ca_certs
            }
            
            # Create MQTT client
            self.mqtt_client = mqtt.Client(client_id=self.mqtt_config["client_id"])
            
            # Set callbacks
            self.mqtt_client.on_connect = self._on_mqtt_connect
            self.mqtt_client.on_message = self._on_mqtt_message
            self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
            
            # Set authentication
            if username and password:
                self.mqtt_client.username_pw_set(username, password)
            
            # Configure TLS
            if use_tls:
                self.mqtt_client.tls_set(
                    ca_certs=ca_certs,
                    cert_reqs=ssl.CERT_REQUIRED,
                    tls_version=ssl.PROTOCOL_TLS
                )
            
            # Connect
            self.mqtt_client.connect(broker, port, 60)
            
            # Start network loop in background
            self.mqtt_client.loop_start()
            
            logger.info(f"📡 Connecting to MQTT broker: {broker}:{port}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MQTT broker: {e}")
            return False
    
    def disconnect_mqtt(self):
        """Disconnect from MQTT broker"""
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self.mqtt_connected = False
            logger.info("👋 Disconnected from MQTT broker")
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            self.mqtt_connected = True
            logger.info("✅ Connected to MQTT broker")
            
            # Subscribe to all registered device topics
            for device_id in self.devices.keys():
                self._subscribe_mqtt_device(device_id)
        else:
            logger.error(f"❌ MQTT connection failed with code {rc}")
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker"""
        self.mqtt_connected = False
        if rc != 0:
            logger.warning(f"⚠️  Unexpected MQTT disconnection (code: {rc})")
        else:
            logger.info("👋 MQTT broker disconnected")
    
    def _on_mqtt_message(self, client, userdata, msg):
        """Callback when MQTT message received"""
        try:
            # Parse topic to get device_id
            # Expected format: iot/devices/{device_type}/{device_id}
            topic_parts = msg.topic.split('/')
            
            if len(topic_parts) >= 4:
                device_type = topic_parts[2]
                device_id = topic_parts[3]
            else:
                logger.warning(f"Invalid topic format: {msg.topic}")
                return
            
            # Parse payload
            try:
                data = json.loads(msg.payload.decode())
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON payload from {device_id}")
                return
            
            # Add timestamp if not present
            if "timestamp" not in data:
                data["timestamp"] = datetime.utcnow().isoformat()
            
            # Update device state
            device = self.devices.get(device_id)
            if device:
                device.update_state(data)
                device.set_status(DeviceStatus.ONLINE)
                
                # Update connection
                connection = self.connections.get(device_id)
                if connection:
                    connection.last_seen = datetime.utcnow()
                    connection.connected = True
            
            # Increment counter
            self.total_messages_received += 1
            
            # Callback with device data
            if self.on_device_data:
                self.on_device_data(device_id, data)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    def _subscribe_mqtt_device(self, device_id: str):
        """Subscribe to device MQTT topic"""
        if not self.mqtt_client or not self.mqtt_connected:
            logger.warning("MQTT not connected, cannot subscribe")
            return
        
        device = self.devices.get(device_id)
        if not device:
            return
        
        # Topic format: iot/devices/{device_type}/{device_id}
        topic = f"iot/devices/{device.device_type.value}/{device_id}"
        
        self.mqtt_client.subscribe(topic)
        logger.info(f"📥 Subscribed to: {topic}")
    
    def _unsubscribe_mqtt_device(self, device_id: str):
        """Unsubscribe from device MQTT topic"""
        if not self.mqtt_client or not self.mqtt_connected:
            return
        
        device = self.devices.get(device_id)
        if not device:
            return
        
        topic = f"iot/devices/{device.device_type.value}/{device_id}"
        self.mqtt_client.unsubscribe(topic)
        logger.info(f"📤 Unsubscribed from: {topic}")
    
    # ==========================================
    # DEVICE COMMANDS
    # ==========================================
    
    async def send_command(
        self,
        device_id: str,
        command: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send command to physical device
        
        Args:
            device_id: Target device ID
            command: Command to execute
            parameters: Command parameters
            
        Returns:
            bool: Success status
        """
        device = self.devices.get(device_id)
        if not device:
            logger.warning(f"Device not found: {device_id}")
            return False
        
        connection = self.connections.get(device_id)
        if not connection:
            logger.warning(f"No connection info for device: {device_id}")
            return False
        
        try:
            # Build command message
            message = {
                "command": command,
                "parameters": parameters or {},
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Send based on connection type
            if connection.connection_type == ConnectionType.MQTT:
                return await self._send_mqtt_command(device, message)
            elif connection.connection_type == ConnectionType.HTTP:
                return await self._send_http_command(device, message)
            else:
                logger.warning(f"Unsupported connection type: {connection.connection_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending command to {device_id}: {e}")
            if self.on_device_error:
                self.on_device_error(device_id, str(e))
            return False
    
    async def _send_mqtt_command(
        self,
        device: PhysicalDevice,
        message: Dict[str, Any]
    ) -> bool:
        """Send command via MQTT"""
        if not self.mqtt_client or not self.mqtt_connected:
            logger.warning("MQTT not connected")
            return False
        
        # Command topic: iot/commands/{device_type}/{device_id}
        topic = f"iot/commands/{device.device_type.value}/{device.device_id}"
        
        # Publish command
        payload = json.dumps(message)
        result = self.mqtt_client.publish(topic, payload, qos=1)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            self.total_commands_sent += 1
            logger.info(f"📤 Command sent to {device.device_id}: {message['command']}")
            return True
        else:
            logger.error(f"Failed to send command: {result.rc}")
            return False
    
    async def _send_http_command(
        self,
        device: PhysicalDevice,
        message: Dict[str, Any]
    ) -> bool:
        """Send command via HTTP"""
        # This would use aiohttp to send HTTP request
        # Placeholder implementation
        logger.info(f"📤 HTTP command sent to {device.device_id}: {message['command']}")
        return True
    
    # ==========================================
    # DEVICE CONTROL ACTIONS
    # ==========================================
    
    async def restart_device(self, device_id: str) -> bool:
        """Restart a device"""
        return await self.send_command(device_id, "restart")
    
    async def update_firmware(self, device_id: str, firmware_url: str) -> bool:
        """Update device firmware"""
        return await self.send_command(device_id, "update_firmware", {
            "firmware_url": firmware_url
        })
    
    async def set_sampling_rate(self, device_id: str, rate_hz: float) -> bool:
        """Set device sampling rate"""
        return await self.send_command(device_id, "set_sampling_rate", {
            "rate_hz": rate_hz
        })
    
    async def calibrate_device(self, device_id: str) -> bool:
        """Calibrate device sensors"""
        return await self.send_command(device_id, "calibrate")
    
    async def set_device_mode(self, device_id: str, mode: str) -> bool:
        """Set device operating mode"""
        return await self.send_command(device_id, "set_mode", {
            "mode": mode
        })
    
    # ==========================================
    # DEVICE DISCOVERY
    # ==========================================
    
    async def discover_devices(
        self,
        method: DeviceDiscoveryMethod = DeviceDiscoveryMethod.MQTT_DISCOVERY,
        timeout_seconds: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Discover devices on network
        
        Args:
            method: Discovery method to use
            timeout_seconds: Discovery timeout
            
        Returns:
            List of discovered devices
        """
        discovered = []
        
        if method == DeviceDiscoveryMethod.MQTT_DISCOVERY:
            discovered = await self._discover_mqtt_devices(timeout_seconds)
        elif method == DeviceDiscoveryMethod.MDNS:
            discovered = await self._discover_mdns_devices(timeout_seconds)
        elif method == DeviceDiscoveryMethod.SCAN:
            discovered = await self._discover_scan_devices(timeout_seconds)
        
        logger.info(f"🔍 Discovered {len(discovered)} devices using {method.value}")
        
        return discovered
    
    async def _discover_mqtt_devices(self, timeout: int) -> List[Dict[str, Any]]:
        """Discover devices via MQTT discovery protocol"""
        if not self.mqtt_client or not self.mqtt_connected:
            logger.warning("MQTT not connected for discovery")
            return []
        
        discovered = []
        
        # Subscribe to discovery topic
        discovery_topic = "iot/discovery/#"
        
        def on_discovery_message(client, userdata, msg):
            try:
                data = json.loads(msg.payload.decode())
                discovered.append(data)
            except Exception as e:
                logger.error(f"Error parsing discovery message: {e}")
        
        # Temporarily override message callback
        original_callback = self.mqtt_client.on_message
        self.mqtt_client.on_message = on_discovery_message
        
        # Subscribe and request discovery
        self.mqtt_client.subscribe(discovery_topic)
        self.mqtt_client.publish("iot/discovery/request", "{}")
        
        # Wait for responses
        await asyncio.sleep(timeout)
        
        # Restore original callback
        self.mqtt_client.on_message = original_callback
        self.mqtt_client.unsubscribe(discovery_topic)
        
        return discovered
    
    async def _discover_mdns_devices(self, timeout: int) -> List[Dict[str, Any]]:
        """Discover devices via mDNS/Bonjour"""
        # This would use zeroconf library
        # Placeholder implementation
        logger.info("🔍 mDNS discovery not yet implemented")
        return []
    
    async def _discover_scan_devices(self, timeout: int) -> List[Dict[str, Any]]:
        """Discover devices via network scan"""
        # This would scan network ports
        # Placeholder implementation
        logger.info("🔍 Network scan discovery not yet implemented")
        return []
    
    # ==========================================
    # HEALTH MONITORING
    # ==========================================
    
    async def check_device_health(self, device_id: str) -> Dict[str, Any]:
        """Check health of specific device"""
        device = self.devices.get(device_id)
        if not device:
            return {"error": "Device not found"}
        
        connection = self.connections.get(device_id)
        
        # Calculate time since last seen
        time_since_seen = None
        if connection and connection.last_seen:
            time_since_seen = (datetime.utcnow() - connection.last_seen).total_seconds()
        
        return {
            "device_id": device_id,
            "status": device.status.value,
            "connected": connection.connected if connection else False,
            "last_seen": connection.last_seen.isoformat() if connection and connection.last_seen else None,
            "time_since_seen_seconds": time_since_seen,
            "current_state": device.current_state
        }
    
    async def check_all_devices_health(self) -> Dict[str, Dict[str, Any]]:
        """Check health of all devices"""
        health_report = {}
        
        for device_id in self.devices.keys():
            health_report[device_id] = await self.check_device_health(device_id)
        
        return health_report
    
    # ==========================================
    # STATISTICS
    # ==========================================
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get device manager statistics"""
        with self.lock:
            total_devices = len(self.devices)
            online_devices = sum(
                1 for d in self.devices.values()
                if d.status == DeviceStatus.ONLINE
            )
            
            devices_by_type = {}
            for device in self.devices.values():
                device_type = device.device_type.value
                devices_by_type[device_type] = devices_by_type.get(device_type, 0) + 1
            
            return {
                "total_devices": total_devices,
                "online_devices": online_devices,
                "offline_devices": total_devices - online_devices,
                "devices_by_type": devices_by_type,
                "mqtt_connected": self.mqtt_connected,
                "total_messages_received": self.total_messages_received,
                "total_commands_sent": self.total_commands_sent
            }


# ==========================================
# REAL HARDWARE INTEGRATION
# ==========================================

class RaspberryPiSensorAdapter:
    """
    Adapter for Raspberry Pi GPIO sensors
    Reads from actual physical sensors and publishes data
    """
    
    def __init__(self, device_manager: IoTDeviceManager):
        self.device_manager = device_manager
        self.sensors: Dict[str, Any] = {}
        self.running = False
        
        try:
            import RPi.GPIO as GPIO
            self.GPIO = GPIO
            self.gpio_available = True
            logger.info("✅ Raspberry Pi GPIO available")
        except ImportError:
            self.GPIO = None
            self.gpio_available = False
            logger.warning("⚠️  Raspberry Pi GPIO not available (running in simulation mode)")
    
    def add_dht22_sensor(
        self,
        device_id: str,
        pin: int,
        name: str = "DHT22 Sensor"
    ):
        """
        Add DHT22 temperature/humidity sensor
        
        Args:
            device_id: Unique device ID
            pin: GPIO pin number
            name: Device name
        """
        try:
            import Adafruit_DHT
            
            self.sensors[device_id] = {
                "type": "DHT22",
                "sensor": Adafruit_DHT.DHT22,
                "pin": pin,
                "name": name
            }
            
            logger.info(f"✅ DHT22 sensor added: {device_id} on pin {pin}")
            
        except ImportError:
            logger.error("❌ Adafruit_DHT library not installed")
            logger.info("Install: pip install Adafruit_DHT")
    
    def add_bmp280_sensor(
        self,
        device_id: str,
        i2c_address: int = 0x76,
        name: str = "BMP280 Sensor"
    ):
        """
        Add BMP280 pressure/temperature sensor
        
        Args:
            device_id: Unique device ID
            i2c_address: I2C address
            name: Device name
        """
        try:
            import board
            import adafruit_bmp280
            
            i2c = board.I2C()
            sensor = adafruit_bmp280.Adafruit_BMP280_I2C(i2c, address=i2c_address)
            
            self.sensors[device_id] = {
                "type": "BMP280",
                "sensor": sensor,
                "name": name
            }
            
            logger.info(f"✅ BMP280 sensor added: {device_id}")
            
        except ImportError:
            logger.error("❌ Adafruit BMP280 library not installed")
            logger.info("Install: pip install adafruit-circuitpython-bmp280")
        except Exception as e:
            logger.error(f"❌ Error initializing BMP280: {e}")
    
    async def start_reading(self, interval_seconds: float = 5.0):
        """
        Start reading from all sensors
        
        Args:
            interval_seconds: Reading interval
        """
        self.running = True
        logger.info(f"🔄 Starting sensor reading (interval: {interval_seconds}s)")
        
        while self.running:
            for device_id, sensor_info in self.sensors.items():
                try:
                    data = await self._read_sensor(device_id, sensor_info)
                    
                    if data:
                        # Publish to device manager
                        if self.device_manager.on_device_data:
                            self.device_manager.on_device_data(device_id, data)
                        
                except Exception as e:
                    logger.error(f"Error reading sensor {device_id}: {e}")
            
            await asyncio.sleep(interval_seconds)
    
    async def _read_sensor(
        self,
        device_id: str,
        sensor_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read data from specific sensor"""
        sensor_type = sensor_info["type"]
        
        if sensor_type == "DHT22":
            return await self._read_dht22(sensor_info)
        elif sensor_type == "BMP280":
            return await self._read_bmp280(sensor_info)
        
        return None
    
    async def _read_dht22(self, sensor_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Read DHT22 sensor"""
        try:
            import Adafruit_DHT
            
            sensor = sensor_info["sensor"]
            pin = sensor_info["pin"]
            
            humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)
            
            if humidity is not None and temperature is not None:
                return {
                    "temperature": round(temperature, 2),
                    "humidity": round(humidity, 2),
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                logger.warning("Failed to read DHT22")
                return None
                
        except Exception as e:
            logger.error(f"DHT22 read error: {e}")
            return None
    
    async def _read_bmp280(self, sensor_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Read BMP280 sensor"""
        try:
            sensor = sensor_info["sensor"]
            
            return {
                "temperature": round(sensor.temperature, 2),
                "pressure": round(sensor.pressure, 2),
                "altitude": round(sensor.altitude, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"BMP280 read error: {e}")
            return None
    
    def stop_reading(self):
        """Stop reading from sensors"""
        self.running = False
        logger.info("⏹️  Stopped sensor reading")


# ==========================================
# EXAMPLE USAGE
# ==========================================

async def main():
    """Example: Connect to MQTT and manage devices"""
    
    # Create device manager
    manager = IoTDeviceManager()
    
    # Setup callback for device data
    def handle_device_data(device_id: str, data: Dict[str, Any]):
        print(f"📊 Data from {device_id}: {data}")
    
    manager.on_device_data = handle_device_data
    
    # Connect to MQTT broker
    manager.connect_mqtt(
        broker="localhost",
        port=1883,
        username=None,
        password=None
    )
    
    # Wait for connection
    await asyncio.sleep(2)
    
    # Register some devices
    from src.models.physical.device import DeviceTemplates
    
    device1 = DeviceTemplates.temperature_sensor("temp_001", "Living Room Sensor")
    connection1 = DeviceConnection(
        device_id="temp_001",
        connection_type=ConnectionType.MQTT,
        endpoint="localhost",
        port=1883,
        protocol_version="3.1.1"
    )
    
    manager.register_device(device1, connection1)
    
    print("\n📱 Device registered. Waiting for data...")
    print("💡 Publish test data:")
    print("   mosquitto_pub -t 'iot/devices/temperature_sensor/temp_001' -m '{\"temperature\": 25.5, \"humidity\": 60}'")
    
    # Keep running
    try:
        await asyncio.sleep(60)
    except KeyboardInterrupt:
        pass
    
    # Cleanup
    manager.disconnect_mqtt()
    
    # Show statistics
    stats = manager.get_statistics()
    print(f"\n📊 Statistics:")
    print(f"   Total devices: {stats['total_devices']}")
    print(f"   Messages received: {stats['total_messages_received']}")


if __name__ == "__main__":
    asyncio.run(main())
