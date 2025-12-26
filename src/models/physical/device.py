"""
Physical Device Model - Represents Real IoT Devices
Enterprise-grade implementation with full lifecycle management
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
import uuid


class DeviceType(Enum):
    """Supported device types"""
    TEMPERATURE_SENSOR = "temperature_sensor"
    HUMIDITY_SENSOR = "humidity_sensor"
    PRESSURE_SENSOR = "pressure_sensor"
    MOTION_DETECTOR = "motion_detector"
    CAMERA = "camera"
    SMART_METER = "smart_meter"
    ACTUATOR = "actuator"
    GATEWAY = "gateway"
    CUSTOM = "custom"


class DeviceStatus(Enum):
    """Device operational status"""
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    ERROR = "error"
    UNKNOWN = "unknown"


class ConnectionType(Enum):
    """Device connection protocols"""
    MQTT = "mqtt"
    HTTP = "http"
    COAP = "coap"
    MODBUS = "modbus"
    OPCUA = "opcua"
    ZIGBEE = "zigbee"
    LORA = "lora"
    BLE = "ble"


@dataclass
class DeviceCapability:
    """Individual device capability/sensor"""
    name: str
    unit: str
    data_type: str  # "float", "int", "bool", "string"
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    precision: Optional[int] = None
    read_only: bool = True
    
    def validate_value(self, value: Any) -> bool:
        """Validate if value is within acceptable range"""
        if self.data_type == "float" or self.data_type == "int":
            if self.min_value is not None and value < self.min_value:
                return False
            if self.max_value is not None and value > self.max_value:
                return False
        return True


@dataclass
class DeviceLocation:
    """Physical location of device"""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    building: Optional[str] = None
    floor: Optional[str] = None
    room: Optional[str] = None
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
            "building": self.building,
            "floor": self.floor,
            "room": self.room,
            "description": self.description
        }


@dataclass
class DeviceMetadata:
    """Device metadata and properties"""
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    firmware_version: Optional[str] = None
    hardware_version: Optional[str] = None
    mac_address: Optional[str] = None
    ip_address: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "manufacturer": self.manufacturer,
            "model": self.model,
            "serial_number": self.serial_number,
            "firmware_version": self.firmware_version,
            "hardware_version": self.hardware_version,
            "mac_address": self.mac_address,
            "ip_address": self.ip_address,
            "tags": self.tags,
            "custom_properties": self.custom_properties
        }


class PhysicalDevice:
    """
    Represents a physical IoT device in the system
    This is the "Physical Twin" that connects to real hardware
    """
    
    def __init__(
        self,
        device_id: str,
        device_type: DeviceType,
        name: str,
        connection_type: ConnectionType,
        capabilities: List[DeviceCapability],
        location: Optional[DeviceLocation] = None,
        metadata: Optional[DeviceMetadata] = None
    ):
        # Core identity
        self.device_id = device_id
        self.device_type = device_type
        self.name = name
        self.connection_type = connection_type
        
        # Capabilities
        self.capabilities = {cap.name: cap for cap in capabilities}
        
        # Location & Metadata
        self.location = location or DeviceLocation()
        self.metadata = metadata or DeviceMetadata()
        
        # State management
        self.status = DeviceStatus.UNKNOWN
        self.last_seen: Optional[datetime] = None
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Current readings
        self.current_state: Dict[str, Any] = {}
        
        # Connection info
        self.mqtt_topic: Optional[str] = None
        self.endpoint: Optional[str] = None
        
        # Relationships
        self.parent_device_id: Optional[str] = None
        self.child_device_ids: List[str] = []
        
    def update_state(self, readings: Dict[str, Any]) -> bool:
        """
        Update device state with new readings
        
        Args:
            readings: Dictionary of capability_name: value
            
        Returns:
            bool: True if all readings are valid
        """
        validated_readings = {}
        
        for capability_name, value in readings.items():
            if capability_name not in self.capabilities:
                print(f"⚠️  Unknown capability: {capability_name}")
                continue
            
            capability = self.capabilities[capability_name]
            
            # Validate value
            if not capability.validate_value(value):
                print(f"⚠️  Invalid value for {capability_name}: {value}")
                continue
            
            validated_readings[capability_name] = value
        
        # Update state
        self.current_state.update(validated_readings)
        self.last_seen = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.status = DeviceStatus.ONLINE
        
        return len(validated_readings) > 0
    
    def get_capability_value(self, capability_name: str) -> Optional[Any]:
        """Get current value of a specific capability"""
        return self.current_state.get(capability_name)
    
    def set_status(self, status: DeviceStatus):
        """Update device status"""
        self.status = status
        self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert device to dictionary"""
        return {
            "device_id": self.device_id,
            "device_type": self.device_type.value,
            "name": self.name,
            "status": self.status.value,
            "connection_type": self.connection_type.value,
            "capabilities": {
                name: {
                    "unit": cap.unit,
                    "data_type": cap.data_type,
                    "min_value": cap.min_value,
                    "max_value": cap.max_value,
                    "read_only": cap.read_only
                }
                for name, cap in self.capabilities.items()
            },
            "current_state": self.current_state,
            "location": self.location.to_dict(),
            "metadata": self.metadata.to_dict(),
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "mqtt_topic": self.mqtt_topic,
            "parent_device_id": self.parent_device_id,
            "child_device_ids": self.child_device_ids
        }
    
    def __repr__(self) -> str:
        return f"PhysicalDevice(id={self.device_id}, type={self.device_type.value}, status={self.status.value})"


# ==========================================
# DEVICE TEMPLATES (Pre-configured devices)
# ==========================================

class DeviceTemplates:
    """Pre-configured device templates for common IoT devices"""
    
    @staticmethod
    def temperature_sensor(device_id: str, name: str) -> PhysicalDevice:
        """Template for temperature sensor (DHT22, DS18B20, etc.)"""
        capabilities = [
            DeviceCapability(
                name="temperature",
                unit="celsius",
                data_type="float",
                min_value=-40.0,
                max_value=125.0,
                precision=2
            ),
            DeviceCapability(
                name="humidity",
                unit="percent",
                data_type="float",
                min_value=0.0,
                max_value=100.0,
                precision=1
            )
        ]
        
        return PhysicalDevice(
            device_id=device_id,
            device_type=DeviceType.TEMPERATURE_SENSOR,
            name=name,
            connection_type=ConnectionType.MQTT,
            capabilities=capabilities
        )
    
    @staticmethod
    def smart_meter(device_id: str, name: str) -> PhysicalDevice:
        """Template for smart energy meter"""
        capabilities = [
            DeviceCapability(
                name="power_consumption",
                unit="watts",
                data_type="float",
                min_value=0.0,
                max_value=10000.0,
                precision=2
            ),
            DeviceCapability(
                name="voltage",
                unit="volts",
                data_type="float",
                min_value=0.0,
                max_value=500.0,
                precision=1
            ),
            DeviceCapability(
                name="current",
                unit="amperes",
                data_type="float",
                min_value=0.0,
                max_value=100.0,
                precision=2
            ),
            DeviceCapability(
                name="energy_total",
                unit="kwh",
                data_type="float",
                min_value=0.0,
                precision=3
            )
        ]
        
        return PhysicalDevice(
            device_id=device_id,
            device_type=DeviceType.SMART_METER,
            name=name,
            connection_type=ConnectionType.MQTT,
            capabilities=capabilities
        )
    
    @staticmethod
    def motion_detector(device_id: str, name: str) -> PhysicalDevice:
        """Template for PIR motion sensor"""
        capabilities = [
            DeviceCapability(
                name="motion_detected",
                unit="boolean",
                data_type="bool"
            ),
            DeviceCapability(
                name="light_level",
                unit="lux",
                data_type="float",
                min_value=0.0,
                max_value=100000.0
            )
        ]
        
        return PhysicalDevice(
            device_id=device_id,
            device_type=DeviceType.MOTION_DETECTOR,
            name=name,
            connection_type=ConnectionType.MQTT,
            capabilities=capabilities
        )
    
    @staticmethod
    def pressure_sensor(device_id: str, name: str) -> PhysicalDevice:
        """Template for barometric pressure sensor (BMP280, etc.)"""
        capabilities = [
            DeviceCapability(
                name="pressure",
                unit="hPa",
                data_type="float",
                min_value=300.0,
                max_value=1100.0,
                precision=2
            ),
            DeviceCapability(
                name="temperature",
                unit="celsius",
                data_type="float",
                min_value=-40.0,
                max_value=85.0,
                precision=2
            ),
            DeviceCapability(
                name="altitude",
                unit="meters",
                data_type="float",
                precision=1
            )
        ]
        
        return PhysicalDevice(
            device_id=device_id,
            device_type=DeviceType.PRESSURE_SENSOR,
            name=name,
            connection_type=ConnectionType.MQTT,
            capabilities=capabilities
        )
    
    @staticmethod
    def camera(device_id: str, name: str) -> PhysicalDevice:
        """Template for IP camera / vision system"""
        capabilities = [
            DeviceCapability(
                name="frame_rate",
                unit="fps",
                data_type="int",
                min_value=1,
                max_value=60
            ),
            DeviceCapability(
                name="resolution",
                unit="pixels",
                data_type="string"
            ),
            DeviceCapability(
                name="object_detected",
                unit="boolean",
                data_type="bool"
            ),
            DeviceCapability(
                name="object_count",
                unit="count",
                data_type="int",
                min_value=0
            )
        ]
        
        return PhysicalDevice(
            device_id=device_id,
            device_type=DeviceType.CAMERA,
            name=name,
            connection_type=ConnectionType.HTTP,
            capabilities=capabilities
        )


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    # Create a temperature sensor
    sensor = DeviceTemplates.temperature_sensor(
        device_id="temp_001",
        name="Living Room Temperature Sensor"
    )
    
    # Set location
    sensor.location = DeviceLocation(
        building="Building A",
        floor="2nd Floor",
        room="Living Room",
        description="Mounted on wall near window"
    )
    
    # Set metadata
    sensor.metadata = DeviceMetadata(
        manufacturer="Bosch",
        model="DHT22",
        serial_number="DHT22-12345",
        firmware_version="1.2.3",
        mac_address="AA:BB:CC:DD:EE:FF"
    )
    
    # Simulate receiving data from physical sensor
    sensor.update_state({
        "temperature": 23.5,
        "humidity": 45.2
    })
    
    print(f"✅ Created device: {sensor}")
    print(f"📊 Current state: {sensor.current_state}")
    print(f"📍 Location: {sensor.location.room}")
