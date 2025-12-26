"""
Digital Twin Engine - Main Orchestration Layer
Enterprise-grade twin lifecycle management and coordination
"""

import asyncio
import threading
from typing import Dict, List, Optional, Set, Callable, Any
from datetime import datetime, timedelta
from collections import defaultdict
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import uuid

from src.models.physical.device import PhysicalDevice, DeviceStatus
from src.models.virtual.twin import DigitalTwin, TwinStatus, HealthStatus


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TwinEngineConfig:
    """Configuration for Twin Engine"""
    
    def __init__(self):
        # Synchronization settings
        self.sync_interval_ms: int = 100  # Target sync latency
        self.max_sync_lag_ms: int = 1000  # Max acceptable lag
        
        # Performance settings
        self.max_twins: int = 100000  # Maximum twins supported
        self.worker_threads: int = 8  # Number of worker threads
        self.batch_size: int = 100  # Batch processing size
        
        # Health monitoring
        self.health_check_interval_sec: int = 30
        self.desync_threshold_sec: int = 60  # Mark as desynced after this
        
        # State management
        self.state_history_size: int = 10000  # States to keep per twin
        self.baseline_recalc_interval: int = 100  # Updates between recalculations
        
        # Persistence
        self.persist_interval_sec: int = 300  # Save to DB every 5 minutes
        self.snapshot_interval_sec: int = 3600  # Full snapshot every hour
        
        # Event handling
        self.enable_event_streaming: bool = True
        self.max_event_queue_size: int = 10000


class TwinMetrics:
    """Metrics and statistics for the engine"""
    
    def __init__(self):
        self.total_twins: int = 0
        self.active_twins: int = 0
        self.syncing_twins: int = 0
        self.desynced_twins: int = 0
        self.archived_twins: int = 0
        
        self.total_syncs: int = 0
        self.failed_syncs: int = 0
        self.sync_success_rate: float = 0.0
        
        self.avg_sync_latency_ms: float = 0.0
        self.max_sync_latency_ms: float = 0.0
        self.min_sync_latency_ms: float = 0.0
        
        self.anomalies_detected: int = 0
        self.simulations_run: int = 0
        
        self.uptime_seconds: float = 0.0
        self.last_updated: datetime = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "twins": {
                "total": self.total_twins,
                "active": self.active_twins,
                "syncing": self.syncing_twins,
                "desynced": self.desynced_twins,
                "archived": self.archived_twins
            },
            "synchronization": {
                "total_syncs": self.total_syncs,
                "failed_syncs": self.failed_syncs,
                "success_rate": self.sync_success_rate,
                "latency": {
                    "avg_ms": self.avg_sync_latency_ms,
                    "max_ms": self.max_sync_latency_ms,
                    "min_ms": self.min_sync_latency_ms
                }
            },
            "analytics": {
                "anomalies_detected": self.anomalies_detected,
                "simulations_run": self.simulations_run
            },
            "system": {
                "uptime_seconds": self.uptime_seconds,
                "last_updated": self.last_updated.isoformat()
            }
        }


class DigitalTwinEngine:
    """
    Main Digital Twin Engine
    
    Responsibilities:
    - Twin lifecycle management (create, update, delete)
    - Real-time state synchronization
    - Multi-twin orchestration
    - Health monitoring
    - Event handling
    - Performance optimization
    """
    
    def __init__(self, config: Optional[TwinEngineConfig] = None):
        # Configuration
        self.config = config or TwinEngineConfig()
        
        # Twin registry
        self.twins: Dict[str, DigitalTwin] = {}
        self.device_to_twin: Dict[str, str] = {}  # device_id -> twin_id mapping
        
        # Physical device registry
        self.physical_devices: Dict[str, PhysicalDevice] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Worker pool
        self.executor = ThreadPoolExecutor(max_workers=self.config.worker_threads)
        
        # Event handling
        self.event_listeners: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=self.config.max_event_queue_size)
        
        # Metrics
        self.metrics = TwinMetrics()
        
        # State management
        self.running = False
        self.start_time: Optional[datetime] = None
        
        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._persistence_task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None
        
        logger.info("🚀 Digital Twin Engine initialized")
        logger.info(f"   Max twins: {self.config.max_twins:,}")
        logger.info(f"   Worker threads: {self.config.worker_threads}")
        logger.info(f"   Sync target: {self.config.sync_interval_ms}ms")
    
    # ==========================================
    # LIFECYCLE MANAGEMENT
    # ==========================================
    
    async def start(self):
        """Start the engine"""
        if self.running:
            logger.warning("Engine already running")
            return
        
        self.running = True
        self.start_time = datetime.utcnow()
        
        # Start background tasks
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._persistence_task = asyncio.create_task(self._persistence_loop())
        self._metrics_task = asyncio.create_task(self._metrics_update_loop())
        
        if self.config.enable_event_streaming:
            asyncio.create_task(self._event_processing_loop())
        
        logger.info("✅ Digital Twin Engine started")
    
    async def stop(self):
        """Stop the engine"""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel background tasks
        if self._health_check_task:
            self._health_check_task.cancel()
        if self._persistence_task:
            self._persistence_task.cancel()
        if self._metrics_task:
            self._metrics_task.cancel()
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        logger.info("👋 Digital Twin Engine stopped")
    
    # ==========================================
    # TWIN CREATION & REGISTRATION
    # ==========================================
    
    def create_twin(
        self,
        device: PhysicalDevice,
        twin_id: Optional[str] = None,
        auto_sync: bool = True
    ) -> DigitalTwin:
        """
        Create a new digital twin for a physical device
        
        Args:
            device: Physical device to twin
            twin_id: Optional custom twin ID
            auto_sync: Enable automatic synchronization
            
        Returns:
            Created DigitalTwin instance
        """
        with self.lock:
            # Check if twin already exists
            if device.device_id in self.device_to_twin:
                existing_twin_id = self.device_to_twin[device.device_id]
                logger.warning(f"Twin already exists for device {device.device_id}: {existing_twin_id}")
                return self.twins[existing_twin_id]
            
            # Check capacity
            if len(self.twins) >= self.config.max_twins:
                raise RuntimeError(f"Maximum twin capacity reached: {self.config.max_twins}")
            
            # Generate twin ID
            if not twin_id:
                twin_id = f"twin_{device.device_id}_{uuid.uuid4().hex[:8]}"
            
            # Create digital twin
            twin = DigitalTwin(
                twin_id=twin_id,
                device_id=device.device_id,
                device_type=device.device_type.value,
                max_history=self.config.state_history_size
            )
            
            # Register twin
            self.twins[twin_id] = twin
            self.device_to_twin[device.device_id] = twin_id
            self.physical_devices[device.device_id] = device
            
            # Update metrics
            self.metrics.total_twins += 1
            self.metrics.active_twins += 1
            
            # Emit event
            self._emit_event("twin_created", {
                "twin_id": twin_id,
                "device_id": device.device_id,
                "device_type": device.device_type.value
            })
            
            logger.info(f"✅ Twin created: {twin_id} for device {device.device_id}")
            
            return twin
    
    def register_physical_device(self, device: PhysicalDevice) -> str:
        """
        Register physical device and create twin
        
        Returns:
            twin_id
        """
        twin = self.create_twin(device)
        return twin.twin_id
    
    def get_twin(self, twin_id: str) -> Optional[DigitalTwin]:
        """Get twin by ID"""
        return self.twins.get(twin_id)
    
    def get_twin_by_device(self, device_id: str) -> Optional[DigitalTwin]:
        """Get twin by physical device ID"""
        twin_id = self.device_to_twin.get(device_id)
        if twin_id:
            return self.twins.get(twin_id)
        return None
    
    def get_all_twins(self) -> List[DigitalTwin]:
        """Get all twins"""
        with self.lock:
            return list(self.twins.values())
    
    def delete_twin(self, twin_id: str, archive: bool = True):
        """
        Delete a twin
        
        Args:
            twin_id: Twin to delete
            archive: If True, archive instead of delete
        """
        with self.lock:
            twin = self.twins.get(twin_id)
            if not twin:
                logger.warning(f"Twin not found: {twin_id}")
                return
            
            if archive:
                twin.archive()
                self.metrics.archived_twins += 1
                logger.info(f"📦 Twin archived: {twin_id}")
            else:
                # Remove from registries
                device_id = twin.device_id
                if device_id in self.device_to_twin:
                    del self.device_to_twin[device_id]
                if device_id in self.physical_devices:
                    del self.physical_devices[device_id]
                del self.twins[twin_id]
                
                self.metrics.total_twins -= 1
                logger.info(f"🗑️  Twin deleted: {twin_id}")
            
            # Emit event
            self._emit_event("twin_deleted", {
                "twin_id": twin_id,
                "archived": archive
            })
    
    # ==========================================
    # STATE SYNCHRONIZATION
    # ==========================================
    
    async def sync_from_physical(
        self,
        device_id: str,
        state_data: Dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Synchronize twin state from physical device
        This is the main sync method called when device data arrives
        
        Args:
            device_id: Physical device ID
            state_data: Current device state
            timestamp: When data was captured
            
        Returns:
            bool: Success status
        """
        # Get twin
        twin = self.get_twin_by_device(device_id)
        if not twin:
            logger.warning(f"No twin found for device {device_id}")
            return False
        
        try:
            # Update twin state
            sync_time = timestamp or datetime.utcnow()
            success = twin.update_from_physical(state_data, sync_time)
            
            if success:
                twin.status = TwinStatus.ACTIVE
                self.metrics.total_syncs += 1
                
                # Update physical device
                if device_id in self.physical_devices:
                    device = self.physical_devices[device_id]
                    device.update_state(state_data)
                
                # Check for anomalies
                anomalies = twin.detect_anomalies()
                if anomalies:
                    self.metrics.anomalies_detected += len(anomalies)
                    self._emit_event("anomaly_detected", {
                        "twin_id": twin.twin_id,
                        "device_id": device_id,
                        "anomalies": anomalies
                    })
                
                # Emit sync event
                self._emit_event("twin_synced", {
                    "twin_id": twin.twin_id,
                    "device_id": device_id,
                    "latency_ms": twin.sync_latency_ms
                })
                
                return True
            else:
                self.metrics.failed_syncs += 1
                twin.status = TwinStatus.ERROR
                return False
                
        except Exception as e:
            logger.error(f"Error syncing twin {twin.twin_id}: {e}")
            self.metrics.failed_syncs += 1
            twin.status = TwinStatus.ERROR
            return False
    
    async def sync_to_physical(
        self,
        device_id: str,
        commands: Dict[str, Any]
    ) -> bool:
        """
        Send commands from twin to physical device (bidirectional sync)
        
        Args:
            device_id: Target device ID
            commands: Commands to send
            
        Returns:
            bool: Success status
        """
        # Get device
        device = self.physical_devices.get(device_id)
        if not device:
            logger.warning(f"Physical device not found: {device_id}")
            return False
        
        try:
            # This would interface with actual device controller
            # For now, log the command
            logger.info(f"📤 Sending commands to device {device_id}: {commands}")
            
            # Emit event
            self._emit_event("command_sent", {
                "device_id": device_id,
                "commands": commands
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending commands to device {device_id}: {e}")
            return False
    
    async def batch_sync(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, bool]:
        """
        Batch synchronization for multiple twins
        
        Args:
            updates: List of {device_id, state_data, timestamp}
            
        Returns:
            Dict of device_id -> success status
        """
        results = {}
        
        # Process in batches
        batch_size = self.config.batch_size
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            # Process batch concurrently
            tasks = [
                self.sync_from_physical(
                    update["device_id"],
                    update["state_data"],
                    update.get("timestamp")
                )
                for update in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect results
            for update, result in zip(batch, batch_results):
                device_id = update["device_id"]
                results[device_id] = result if not isinstance(result, Exception) else False
        
        return results
    
    # ==========================================
    # HEALTH MONITORING
    # ==========================================
    
    async def _health_check_loop(self):
        """Background task for health monitoring"""
        logger.info("🏥 Health check loop started")
        
        while self.running:
            try:
                await asyncio.sleep(self.config.health_check_interval_sec)
                await self._check_twin_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
    
    async def _check_twin_health(self):
        """Check health of all twins"""
        now = datetime.utcnow()
        desync_threshold = timedelta(seconds=self.config.desync_threshold_sec)
        
        with self.lock:
            for twin in self.twins.values():
                # Check if twin is desynced
                if twin.last_sync:
                    time_since_sync = now - twin.last_sync
                    if time_since_sync > desync_threshold:
                        if twin.status != TwinStatus.DESYNC:
                            twin.status = TwinStatus.DESYNC
                            self.metrics.desynced_twins += 1
                            logger.warning(f"⚠️  Twin desynced: {twin.twin_id} (last sync: {time_since_sync.seconds}s ago)")
                            
                            self._emit_event("twin_desynced", {
                                "twin_id": twin.twin_id,
                                "time_since_sync": time_since_sync.seconds
                            })
    
    # ==========================================
    # ANALYTICS & QUERIES
    # ==========================================
    
    def get_twins_by_status(self, status: TwinStatus) -> List[DigitalTwin]:
        """Get all twins with specific status"""
        with self.lock:
            return [twin for twin in self.twins.values() if twin.status == status]
    
    def get_twins_by_health(self, health: HealthStatus) -> List[DigitalTwin]:
        """Get all twins with specific health status"""
        with self.lock:
            return [twin for twin in self.twins.values() if twin.health_status == health]
    
    def get_twins_by_device_type(self, device_type: str) -> List[DigitalTwin]:
        """Get all twins of specific device type"""
        with self.lock:
            return [twin for twin in self.twins.values() if twin.device_type == device_type]
    
    def get_anomalous_twins(self) -> List[DigitalTwin]:
        """Get all twins with current anomalies"""
        with self.lock:
            return [twin for twin in self.twins.values() if twin.current_anomalies]
    
    def search_twins(self, filters: Dict[str, Any]) -> List[DigitalTwin]:
        """
        Search twins with complex filters
        
        Example filters:
        {
            "device_type": "temperature_sensor",
            "health_status": "critical",
            "min_anomaly_count": 5,
            "tags": ["production", "building-a"]
        }
        """
        results = []
        
        with self.lock:
            for twin in self.twins.values():
                # Check each filter
                match = True
                
                if "device_type" in filters and twin.device_type != filters["device_type"]:
                    match = False
                
                if "status" in filters and twin.status.value != filters["status"]:
                    match = False
                
                if "health_status" in filters and twin.health_status.value != filters["health_status"]:
                    match = False
                
                if "min_anomaly_count" in filters and twin.anomaly_count < filters["min_anomaly_count"]:
                    match = False
                
                if "tags" in filters:
                    required_tags = set(filters["tags"])
                    twin_tags = set(twin.tags)
                    if not required_tags.issubset(twin_tags):
                        match = False
                
                if match:
                    results.append(twin)
        
        return results
    
    # ==========================================
    # BULK OPERATIONS
    # ==========================================
    
    async def calculate_all_baselines(self):
        """Calculate baselines for all twins"""
        logger.info("📊 Calculating baselines for all twins...")
        
        tasks = []
        for twin in self.twins.values():
            task = asyncio.create_task(asyncio.to_thread(twin.calculate_baselines))
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"✅ Baselines calculated for {len(tasks)} twins")
    
    async def detect_all_anomalies(self) -> Dict[str, List[Dict[str, Any]]]:
        """Detect anomalies in all twins"""
        results = {}
        
        for twin in self.twins.values():
            anomalies = twin.detect_anomalies()
            if anomalies:
                results[twin.twin_id] = anomalies
        
        return results
    
    async def simulate_all_twins(
        self,
        scenario_type: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Run simulation on all twins"""
        results = []
        
        tasks = []
        for twin in self.twins.values():
            task = asyncio.create_task(
                asyncio.to_thread(twin.simulate_scenario, scenario_type, parameters)
            )
            tasks.append((twin.twin_id, task))
        
        # Wait for all simulations
        for twin_id, task in tasks:
            try:
                result = await task
                results.append({
                    "twin_id": twin_id,
                    "result": result.to_dict()
                })
                self.metrics.simulations_run += 1
            except Exception as e:
                logger.error(f"Simulation failed for {twin_id}: {e}")
        
        return results
    
    # ==========================================
    # EVENT SYSTEM
    # ==========================================
    
    def on(self, event_type: str, callback: Callable):
        """Register event listener"""
        self.event_listeners[event_type].append(callback)
    
    def off(self, event_type: str, callback: Callable):
        """Unregister event listener"""
        if event_type in self.event_listeners:
            self.event_listeners[event_type].remove(callback)
    
    def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit event to listeners"""
        event = {
            "type": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Call synchronous listeners
        for callback in self.event_listeners.get(event_type, []):
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in event listener: {e}")
        
        # Add to event queue for async processing
        if self.config.enable_event_streaming:
            try:
                self.event_queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Event queue full, dropping event")
    
    async def _event_processing_loop(self):
        """Background task for processing events"""
        logger.info("📡 Event processing loop started")
        
        while self.running:
            try:
                event = await self.event_queue.get()
                # Process event (e.g., send to external system, log, etc.)
                logger.debug(f"Event: {event['type']}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing event: {e}")
    
    # ==========================================
    # METRICS & STATISTICS
    # ==========================================
    
    async def _metrics_update_loop(self):
        """Background task for updating metrics"""
        logger.info("📊 Metrics update loop started")
        
        while self.running:
            try:
                await asyncio.sleep(5)  # Update every 5 seconds
                self._update_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating metrics: {e}")
    
    def _update_metrics(self):
        """Update engine metrics"""
        with self.lock:
            # Count twins by status
            active = 0
            syncing = 0
            desynced = 0
            
            # Collect sync latencies
            latencies = []
            
            for twin in self.twins.values():
                if twin.status == TwinStatus.ACTIVE:
                    active += 1
                elif twin.status == TwinStatus.SYNCING:
                    syncing += 1
                elif twin.status == TwinStatus.DESYNC:
                    desynced += 1
                
                if twin.sync_latency_ms > 0:
                    latencies.append(twin.sync_latency_ms)
            
            # Update metrics
            self.metrics.active_twins = active
            self.metrics.syncing_twins = syncing
            self.metrics.desynced_twins = desynced
            
            # Calculate latency statistics
            if latencies:
                self.metrics.avg_sync_latency_ms = sum(latencies) / len(latencies)
                self.metrics.max_sync_latency_ms = max(latencies)
                self.metrics.min_sync_latency_ms = min(latencies)
            
            # Calculate success rate
            if self.metrics.total_syncs > 0:
                self.metrics.sync_success_rate = (
                    (self.metrics.total_syncs - self.metrics.failed_syncs) / 
                    self.metrics.total_syncs
                )
            
            # Update uptime
            if self.start_time:
                self.metrics.uptime_seconds = (datetime.utcnow() - self.start_time).total_seconds()
            
            self.metrics.last_updated = datetime.utcnow()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.metrics.to_dict()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get detailed statistics"""
        with self.lock:
            return {
                "engine": {
                    "running": self.running,
                    "uptime_seconds": self.metrics.uptime_seconds,
                    "start_time": self.start_time.isoformat() if self.start_time else None
                },
                "twins": {
                    "total": len(self.twins),
                    "by_status": {
                        status.value: len(self.get_twins_by_status(status))
                        for status in TwinStatus
                    },
                    "by_health": {
                        health.value: len(self.get_twins_by_health(health))
                        for health in HealthStatus
                    }
                },
                "performance": {
                    "sync_success_rate": self.metrics.sync_success_rate,
                    "avg_latency_ms": self.metrics.avg_sync_latency_ms,
                    "total_syncs": self.metrics.total_syncs,
                    "failed_syncs": self.metrics.failed_syncs
                },
                "analytics": {
                    "anomalies_detected": self.metrics.anomalies_detected,
                    "simulations_run": self.metrics.simulations_run,
                    "anomalous_twins": len(self.get_anomalous_twins())
                }
            }
    
    # ==========================================
    # PERSISTENCE
    # ==========================================
    
    async def _persistence_loop(self):
        """Background task for periodic persistence"""
        logger.info("💾 Persistence loop started")
        
        while self.running:
            try:
                await asyncio.sleep(self.config.persist_interval_sec)
                await self._persist_twins()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in persistence loop: {e}")
    
    async def _persist_twins(self):
        """Persist twin states to database"""
        logger.info("💾 Persisting twin states...")
        
        # This would save to actual database
        # For now, just log
        with self.lock:
            twin_count = len(self.twins)
        
        logger.info(f"✅ Persisted {twin_count} twin states")
    
    def export_twin(self, twin_id: str) -> Optional[Dict[str, Any]]:
        """Export complete twin data"""
        twin = self.get_twin(twin_id)
        if not twin:
            return None
        
        return twin.to_dict()
    
    def export_all_twins(self) -> List[Dict[str, Any]]:
        """Export all twins"""
        with self.lock:
            return [twin.to_dict() for twin in self.twins.values()]
    
    # ==========================================
    # UTILITY METHODS
    # ==========================================
    
    def __repr__(self) -> str:
        return f"DigitalTwinEngine(twins={len(self.twins)}, running={self.running})"
    
    def __len__(self) -> int:
        return len(self.twins)


# ==========================================
# EXAMPLE USAGE
# ==========================================

async def main():
    """Example usage of Digital Twin Engine"""
    
    # Create engine
    config = TwinEngineConfig()
    config.sync_interval_ms = 50
    config.worker_threads = 4
    
    engine = DigitalTwinEngine(config)
    
    # Start engine
    await engine.start()
    
    # Create some physical devices
    from src.models.physical.device import DeviceTemplates
    
    devices = [
        DeviceTemplates.temperature_sensor(f"temp_{i:03d}", f"Temp Sensor {i}")
        for i in range(10)
    ]
    
    # Register devices and create twins
    print("\n📱 Creating twins...")
    for device in devices:
        twin = engine.create_twin(device)
        print(f"  ✅ {twin.twin_id}")
    
    # Simulate data coming from physical devices
    print("\n🔄 Simulating device data...")
    for i in range(20):
        updates = []
        for device in devices:
            state = {
                "temperature": 25.0 + np.random.normal(0, 2),
                "humidity": 50.0 + np.random.normal(0, 5)
            }
            updates.append({
                "device_id": device.device_id,
                "state_data": state,
                "timestamp": datetime.utcnow()
            })
        
        # Batch sync
        results = await engine.batch_sync(updates)
        success_count = sum(1 for r in results.values() if r)
        print(f"  Sync #{i+1}: {success_count}/{len(results)} successful")
        
        await asyncio.sleep(0.1)
    
    # Calculate baselines
    print("\n📊 Calculating baselines...")
    await engine.calculate_all_baselines()
    
    # Detect anomalies
    print("\n🔍 Detecting anomalies...")
    anomalies = await engine.detect_all_anomalies()
    print(f"  Found anomalies in {len(anomalies)} twins")
    
    # Get metrics
    print("\n📈 Engine metrics:")
    stats = engine.get_statistics()
    print(f"  Total twins: {stats['twins']['total']}")
    print(f"  Sync success rate: {stats['performance']['sync_success_rate']*100:.1f}%")
    print(f"  Avg latency: {stats['performance']['avg_latency_ms']:.2f}ms")
    print(f"  Anomalies detected: {stats['analytics']['anomalies_detected']}")
    
    # Stop engine
    await engine.stop()
    
    print("\n✅ Demo complete!")


if __name__ == "__main__":
    import numpy as np
    asyncio.run(main())
