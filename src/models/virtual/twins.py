"""
Digital Twin Model - Virtual Representation
Complete implementation of Digital Twin with state, behavior, and lifecycle
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from collections import deque
import statistics
import json
import numpy as np


class TwinStatus(Enum):
    """Digital twin status"""
    CREATING = "creating"
    ACTIVE = "active"
    SYNCING = "syncing"
    DESYNC = "desync"
    SIMULATING = "simulating"
    MAINTENANCE = "maintenance"
    ARCHIVED = "archived"
    ERROR = "error"


class HealthStatus(Enum):
    """Twin health assessment"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class SyncQuality(Enum):
    """Synchronization quality with physical twin"""
    EXCELLENT = "excellent"  # <50ms latency
    GOOD = "good"            # <100ms latency
    ACCEPTABLE = "acceptable" # <500ms latency
    POOR = "poor"            # >500ms latency
    LOST = "lost"            # No sync


@dataclass
class TwinState:
    """Represents a single state snapshot of the twin"""
    timestamp: datetime
    properties: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "properties": self.properties,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TwinState':
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            properties=data["properties"],
            metadata=data.get("metadata", {})
        )


@dataclass
class BaselineProfile:
    """Statistical baseline of normal behavior"""
    capability: str
    mean: float
    std_dev: float
    min_value: float
    max_value: float
    percentile_25: float
    percentile_50: float
    percentile_75: float
    sample_count: int
    last_updated: datetime
    
    def is_anomalous(self, value: float, sigma_threshold: float = 3.0) -> Tuple[bool, float]:
        """
        Check if value is anomalous
        
        Returns:
            (is_anomalous, deviation_score)
        """
        if self.std_dev == 0:
            return False, 0.0
        
        deviation = abs(value - self.mean) / self.std_dev
        return deviation > sigma_threshold, deviation
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "capability": self.capability,
            "mean": self.mean,
            "std_dev": self.std_dev,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "percentile_25": self.percentile_25,
            "percentile_50": self.percentile_50,
            "percentile_75": self.percentile_75,
            "sample_count": self.sample_count,
            "last_updated": self.last_updated.isoformat()
        }


@dataclass
class SimulationResult:
    """Result from twin simulation"""
    simulation_id: str
    simulation_type: str
    input_parameters: Dict[str, Any]
    predicted_state: Dict[str, Any]
    confidence: float
    risk_score: float
    recommendations: List[str]
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "simulation_id": self.simulation_id,
            "simulation_type": self.simulation_type,
            "input_parameters": self.input_parameters,
            "predicted_state": self.predicted_state,
            "confidence": self.confidence,
            "risk_score": self.risk_score,
            "recommendations": self.recommendations,
            "timestamp": self.timestamp.isoformat()
        }


class DigitalTwin:
    """
    Complete Digital Twin Implementation
    
    Represents virtual replica of physical device with:
    - Real-time state synchronization
    - Historical state management
    - Baseline behavior learning
    - Anomaly detection
    - Predictive simulation
    - Lifecycle management
    """
    
    def __init__(
        self,
        twin_id: str,
        device_id: str,
        device_type: str,
        max_history: int = 10000
    ):
        # Identity
        self.twin_id = twin_id
        self.device_id = device_id
        self.device_type = device_type
        
        # Status
        self.status = TwinStatus.CREATING
        self.health_status = HealthStatus.UNKNOWN
        self.sync_quality = SyncQuality.LOST
        
        # Timestamps
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.last_sync: Optional[datetime] = None
        
        # Current state
        self.current_state: Dict[str, Any] = {}
        self.previous_state: Dict[str, Any] = {}
        
        # State history (circular buffer)
        self.state_history: deque = deque(maxlen=max_history)
        self.max_history = max_history
        
        # Baseline profiles
        self.baselines: Dict[str, BaselineProfile] = {}
        self.baseline_calculation_interval = 100  # Recalculate every N updates
        self.update_count = 0
        
        # Synchronization metrics
        self.sync_latency_ms: float = 0.0
        self.sync_success_rate: float = 0.0
        self.total_syncs: int = 0
        self.failed_syncs: int = 0
        
        # Anomaly tracking
        self.anomaly_count: int = 0
        self.current_anomalies: List[Dict[str, Any]] = []
        
        # Simulation history
        self.simulations: List[SimulationResult] = []
        
        # Metadata
        self.tags: List[str] = []
        self.properties: Dict[str, Any] = {}
        
        # Change tracking
        self.change_log: List[Dict[str, Any]] = []
        
        print(f"✅ Digital Twin created: {self.twin_id} for device {self.device_id}")
    
    # ==========================================
    # STATE SYNCHRONIZATION
    # ==========================================
    
    def update_from_physical(
        self,
        physical_state: Dict[str, Any],
        sync_timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Update twin state from physical device
        This is the core synchronization method
        
        Args:
            physical_state: Current state from physical device
            sync_timestamp: When data was captured (for latency calculation)
            
        Returns:
            bool: Success status
        """
        try:
            # Calculate sync latency
            now = datetime.utcnow()
            if sync_timestamp:
                latency = (now - sync_timestamp).total_seconds() * 1000
                self.sync_latency_ms = latency
                self._update_sync_quality(latency)
            
            # Store previous state
            self.previous_state = self.current_state.copy()
            
            # Update current state
            self.current_state = physical_state.copy()
            self.updated_at = now
            self.last_sync = now
            
            # Add to history
            state_snapshot = TwinState(
                timestamp=now,
                properties=physical_state.copy(),
                metadata={
                    "sync_latency_ms": self.sync_latency_ms,
                    "update_count": self.update_count
                }
            )
            self.state_history.append(state_snapshot)
            
            # Track changes
            changes = self._detect_changes(self.previous_state, self.current_state)
            if changes:
                self.change_log.append({
                    "timestamp": now.isoformat(),
                    "changes": changes
                })
            
            # Increment counters
            self.update_count += 1
            self.total_syncs += 1
            
            # Update sync success rate
            self.sync_success_rate = (self.total_syncs - self.failed_syncs) / self.total_syncs
            
            # Recalculate baselines periodically
            if self.update_count % self.baseline_calculation_interval == 0:
                self.calculate_baselines()
            
            # Update status
            if self.status == TwinStatus.CREATING:
                self.status = TwinStatus.ACTIVE
            
            return True
            
        except Exception as e:
            print(f"❌ Error updating twin {self.twin_id}: {e}")
            self.failed_syncs += 1
            self.status = TwinStatus.ERROR
            return False
    
    def _update_sync_quality(self, latency_ms: float):
        """Update sync quality based on latency"""
        if latency_ms < 50:
            self.sync_quality = SyncQuality.EXCELLENT
        elif latency_ms < 100:
            self.sync_quality = SyncQuality.GOOD
        elif latency_ms < 500:
            self.sync_quality = SyncQuality.ACCEPTABLE
        else:
            self.sync_quality = SyncQuality.POOR
    
    def _detect_changes(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Detect what changed between states"""
        changes = {}
        
        for key, new_value in new_state.items():
            old_value = old_state.get(key)
            if old_value != new_value:
                changes[key] = {
                    "old": old_value,
                    "new": new_value,
                    "delta": new_value - old_value if isinstance(new_value, (int, float)) and isinstance(old_value, (int, float)) else None
                }
        
        return changes
    
    # ==========================================
    # BASELINE LEARNING
    # ==========================================
    
    def calculate_baselines(self):
        """
        Calculate statistical baselines from historical data
        This learns what is "normal" for this twin
        """
        if len(self.state_history) < 10:
            print(f"⏳ Not enough data for {self.twin_id} (need 10+, have {len(self.state_history)})")
            return
        
        # Collect all numeric values by capability
        capability_values: Dict[str, List[float]] = {}
        
        for state in self.state_history:
            for key, value in state.properties.items():
                if isinstance(value, (int, float)):
                    if key not in capability_values:
                        capability_values[key] = []
                    capability_values[key].append(float(value))
        
        # Calculate baseline for each capability
        for capability, values in capability_values.items():
            if len(values) < 10:
                continue
            
            values_array = np.array(values)
            
            baseline = BaselineProfile(
                capability=capability,
                mean=float(np.mean(values_array)),
                std_dev=float(np.std(values_array)),
                min_value=float(np.min(values_array)),
                max_value=float(np.max(values_array)),
                percentile_25=float(np.percentile(values_array, 25)),
                percentile_50=float(np.percentile(values_array, 50)),
                percentile_75=float(np.percentile(values_array, 75)),
                sample_count=len(values),
                last_updated=datetime.utcnow()
            )
            
            self.baselines[capability] = baseline
        
        print(f"📊 Baselines calculated for {self.twin_id}: {len(self.baselines)} capabilities")
    
    # ==========================================
    # ANOMALY DETECTION
    # ==========================================
    
    def detect_anomalies(self, sigma_threshold: float = 3.0) -> List[Dict[str, Any]]:
        """
        Detect anomalies in current state compared to baseline
        
        Args:
            sigma_threshold: Number of standard deviations for anomaly
            
        Returns:
            List of detected anomalies
        """
        if not self.baselines:
            return []
        
        anomalies = []
        
        for capability, value in self.current_state.items():
            if not isinstance(value, (int, float)):
                continue
            
            if capability not in self.baselines:
                continue
            
            baseline = self.baselines[capability]
            is_anomalous, deviation = baseline.is_anomalous(value, sigma_threshold)
            
            if is_anomalous:
                anomaly = {
                    "capability": capability,
                    "current_value": value,
                    "expected_mean": baseline.mean,
                    "expected_range": (
                        baseline.mean - sigma_threshold * baseline.std_dev,
                        baseline.mean + sigma_threshold * baseline.std_dev
                    ),
                    "deviation_score": deviation,
                    "timestamp": datetime.utcnow().isoformat()
                }
                anomalies.append(anomaly)
                self.anomaly_count += 1
        
        self.current_anomalies = anomalies
        
        # Update health status based on anomalies
        if len(anomalies) == 0:
            self.health_status = HealthStatus.HEALTHY
        elif len(anomalies) <= 2:
            self.health_status = HealthStatus.WARNING
        else:
            self.health_status = HealthStatus.CRITICAL
        
        return anomalies
    
    # ==========================================
    # PREDICTIVE SIMULATION
    # ==========================================
    
    def simulate_scenario(
        self,
        scenario_type: str,
        parameters: Dict[str, Any]
    ) -> SimulationResult:
        """
        Simulate a what-if scenario
        
        Args:
            scenario_type: Type of simulation (e.g., "overload", "failure", "attack")
            parameters: Simulation parameters
            
        Returns:
            SimulationResult with predicted outcome
        """
        simulation_id = f"sim_{self.twin_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        # Start from current state
        predicted_state = self.current_state.copy()
        confidence = 0.8
        risk_score = 0.0
        recommendations = []
        
        # Scenario-specific simulation logic
        if scenario_type == "overload":
            # Simulate device overload
            load_increase = parameters.get("load_increase_percent", 50)
            
            for key in ["cpu_usage", "memory_usage", "temperature"]:
                if key in predicted_state:
                    predicted_state[key] *= (1 + load_increase / 100)
            
            # Calculate risk
            if predicted_state.get("cpu_usage", 0) > 90:
                risk_score = 0.9
                recommendations.append("Reduce workload immediately")
                recommendations.append("Enable load balancing")
            elif predicted_state.get("cpu_usage", 0) > 70:
                risk_score = 0.6
                recommendations.append("Monitor closely")
                recommendations.append("Prepare for scaling")
            
        elif scenario_type == "failure":
            # Simulate component failure
            component = parameters.get("component", "sensor")
            
            if component == "sensor":
                # Sensor failure → data becomes erratic
                for key in predicted_state:
                    if isinstance(predicted_state[key], (int, float)):
                        predicted_state[key] = predicted_state[key] * np.random.uniform(0.5, 1.5)
                
                risk_score = 0.8
                recommendations.append(f"Replace {component}")
                recommendations.append("Schedule maintenance")
            
        elif scenario_type == "attack":
            # Simulate cyber attack
            attack_type = parameters.get("attack_type", "ddos")
            
            if attack_type == "ddos":
                predicted_state["packet_rate"] = predicted_state.get("packet_rate", 100) * 100
                predicted_state["cpu_usage"] = 98
                predicted_state["memory_usage"] = 95
                
                risk_score = 0.95
                recommendations.append("Isolate device from network")
                recommendations.append("Enable DDoS protection")
                recommendations.append("Block suspicious IPs")
        
        # Create result
        result = SimulationResult(
            simulation_id=simulation_id,
            simulation_type=scenario_type,
            input_parameters=parameters,
            predicted_state=predicted_state,
            confidence=confidence,
            risk_score=risk_score,
            recommendations=recommendations,
            timestamp=datetime.utcnow()
        )
        
        self.simulations.append(result)
        
        print(f"🔮 Simulation completed: {scenario_type} (risk: {risk_score:.2f})")
        
        return result
    
    # ==========================================
    # QUERY & ANALYSIS
    # ==========================================
    
    def get_state_at_time(self, timestamp: datetime) -> Optional[TwinState]:
        """Get twin state at specific time"""
        for state in reversed(self.state_history):
            if state.timestamp <= timestamp:
                return state
        return None
    
    def get_state_range(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[TwinState]:
        """Get all states within time range"""
        return [
            state for state in self.state_history
            if start_time <= state.timestamp <= end_time
        ]
    
    def get_capability_history(
        self,
        capability: str,
        duration_minutes: int = 60
    ) -> List[Tuple[datetime, Any]]:
        """Get history of specific capability"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=duration_minutes)
        
        history = []
        for state in self.state_history:
            if state.timestamp >= cutoff_time:
                value = state.properties.get(capability)
                if value is not None:
                    history.append((state.timestamp, value))
        
        return history
    
    def calculate_statistics(
        self,
        capability: str,
        duration_minutes: int = 60
    ) -> Dict[str, float]:
        """Calculate statistics for capability over time period"""
        history = self.get_capability_history(capability, duration_minutes)
        
        if not history:
            return {}
        
        values = [v for _, v in history if isinstance(v, (int, float))]
        
        if not values:
            return {}
        
        return {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "std_dev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "min": min(values),
            "max": max(values),
            "count": len(values)
        }
    
    # ==========================================
    # LIFECYCLE MANAGEMENT
    # ==========================================
    
    def archive(self):
        """Archive this twin (soft delete)"""
        self.status = TwinStatus.ARCHIVED
        self.updated_at = datetime.utcnow()
    
    def reset(self):
        """Reset twin to initial state"""
        self.current_state = {}
        self.previous_state = {}
        self.state_history.clear()
        self.baselines.clear()
        self.current_anomalies.clear()
        self.update_count = 0
        self.anomaly_count = 0
        self.status = TwinStatus.CREATING
        self.health_status = HealthStatus.UNKNOWN
    
    # ==========================================
    # SERIALIZATION
    # ==========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert twin to dictionary"""
        return {
            "twin_id": self.twin_id,
            "device_id": self.device_id,
            "device_type": self.device_type,
            "status": self.status.value,
            "health_status": self.health_status.value,
            "sync_quality": self.sync_quality.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
            "current_state": self.current_state,
            "sync_metrics": {
                "latency_ms": self.sync_latency_ms,
                "success_rate": self.sync_success_rate,
                "total_syncs": self.total_syncs,
                "failed_syncs": self.failed_syncs
            },
            "baselines": {
                k: v.to_dict() for k, v in self.baselines.items()
            },
            "anomalies": {
                "current": self.current_anomalies,
                "total_count": self.anomaly_count
            },
            "statistics": {
                "update_count": self.update_count,
                "history_length": len(self.state_history),
                "simulation_count": len(self.simulations)
            },
            "tags": self.tags,
            "properties": self.properties
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    def __repr__(self) -> str:
        return f"DigitalTwin(id={self.twin_id}, device={self.device_id}, status={self.status.value}, health={self.health_status.value})"


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    # Create a digital twin
    twin = DigitalTwin(
        twin_id="twin_001",
        device_id="temp_sensor_001",
        device_type="temperature_sensor"
    )
    
    # Simulate receiving data from physical device
    for i in range(150):
        # Normal readings
        twin.update_from_physical({
            "temperature": 25.0 + np.random.normal(0, 0.5),
            "humidity": 50.0 + np.random.normal(0, 2),
            "cpu_usage": 30.0 + np.random.normal(0, 3)
        })
    
    # Check baselines
    print(f"\n📊 Baselines:")
    for cap, baseline in twin.baselines.items():
        print(f"  {cap}: {baseline.mean:.2f} ± {baseline.std_dev:.2f}")
    
    # Simulate anomalous reading
    twin.update_from_physical({
        "temperature": 85.0,  # Abnormal!
        "humidity": 95.0,     # Abnormal!
        "cpu_usage": 98.0     # Abnormal!
    })
    
    # Detect anomalies
    anomalies = twin.detect_anomalies()
    print(f"\n⚠️  Anomalies detected: {len(anomalies)}")
    for anomaly in anomalies:
        print(f"  {anomaly['capability']}: {anomaly['current_value']:.2f} (expected: {anomaly['expected_mean']:.2f})")
    
    # Run simulation
    result = twin.simulate_scenario("overload", {"load_increase_percent": 80})
    print(f"\n🔮 Simulation result:")
    print(f"  Risk Score: {result.risk_score:.2f}")
    print(f"  Recommendations: {', '.join(result.recommendations)}")
    
    # Get twin summary
    print(f"\n📋 Twin Summary:")
    print(f"  Updates: {twin.update_count}")
    print(f"  Sync Success Rate: {twin.sync_success_rate * 100:.1f}%")
    print(f"  Health: {twin.health_status.value}")
    print(f"  Sync Quality: {twin.sync_quality.value}")
