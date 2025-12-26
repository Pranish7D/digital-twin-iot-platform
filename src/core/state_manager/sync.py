"""
State Synchronization Service
Handles bidirectional sync between physical and digital twins
"""

import asyncio
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class SyncOperation:
    """Represents a single sync operation"""
    operation_id: str
    direction: str  # "physical_to_digital" or "digital_to_physical"
    device_id: str
    twin_id: str
    data: Dict[str, Any]
    timestamp: datetime
    status: str = "pending"  # pending, in_progress, completed, failed
    latency_ms: float = 0.0
    error: Optional[str] = None


class SyncStrategy:
    """Base class for synchronization strategies"""
    
    def should_sync(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> bool:
        """Determine if sync is needed"""
        raise NotImplementedError


class AlwaysSyncStrategy(SyncStrategy):
    """Always synchronize on every update"""
    
    def should_sync(self, old_state, new_state) -> bool:
        return True


class DeltaSyncStrategy(SyncStrategy):
    """Sync only when values change significantly"""
    
    def __init__(self, threshold: float = 0.01):
        self.threshold = threshold
    
    def should_sync(self, old_state, new_state) -> bool:
        for key, new_value in new_state.items():
            old_value = old_state.get(key)
            
            if old_value is None:
                return True
            
            if isinstance(new_value, (int, float)) and isinstance(old_value, (int, float)):
                delta = abs(new_value - old_value) / (abs(old_value) + 1e-10)
                if delta > self.threshold:
                    return True
            elif new_value != old_value:
                return True
        
        return False


class TimerSyncStrategy(SyncStrategy):
    """Sync at regular intervals regardless of changes"""
    
    def __init__(self, interval_seconds: float = 1.0):
        self.interval = interval_seconds
        self.last_sync: Dict[str, datetime] = {}
    
    def should_sync(self, old_state, new_state) -> bool:
        # This would need context (device_id) to work properly
        # Simplified for now
        return True


class StateSync:
    """
    State Synchronization Service
    
    Manages bidirectional synchronization between:
    - Physical devices → Digital twins (telemetry)
    - Digital twins → Physical devices (commands)
    """
    
    def __init__(self, strategy: Optional[SyncStrategy] = None):
        self.strategy = strategy or DeltaSyncStrategy()
        
        # Operation tracking
        self.pending_operations: deque = deque(maxlen=10000)
        self.completed_operations: deque = deque(maxlen=1000)
        
        # Callbacks
        self.on_sync_start: Optional[Callable] = None
        self.on_sync_complete: Optional[Callable] = None
        self.on_sync_error: Optional[Callable] = None
        
        # Metrics
        self.total_syncs = 0
        self.successful_syncs = 0
        self.failed_syncs = 0
        
        # Conflict resolution
        self.conflict_resolver: Optional[Callable] = None
    
    async def sync_physical_to_digital(
        self,
        device_id: str,
        twin_id: str,
        physical_state: Dict[str, Any],
        previous_state: Dict[str, Any]
    ) -> SyncOperation:
        """
        Synchronize state from physical device to digital twin
        
        Args:
            device_id: Physical device ID
            twin_id: Digital twin ID
            physical_state: Current physical device state
            previous_state: Previous state for delta detection
            
        Returns:
            SyncOperation result
        """
        operation = SyncOperation(
            operation_id=f"sync_{device_id}_{datetime.utcnow().timestamp()}",
            direction="physical_to_digital",
            device_id=device_id,
            twin_id=twin_id,
            data=physical_state,
            timestamp=datetime.utcnow()
        )
        
        try:
            # Check if sync is needed
            if not self.strategy.should_sync(previous_state, physical_state):
                operation.status = "skipped"
                return operation
            
            # Start sync
            operation.status = "in_progress"
            start_time = datetime.utcnow()
            
            if self.on_sync_start:
                self.on_sync_start(operation)
            
            # Perform sync (this would call twin.update_from_physical())
            # Simulated here
            await asyncio.sleep(0.001)  # Simulate network latency
            
            # Calculate latency
            end_time = datetime.utcnow()
            operation.latency_ms = (end_time - start_time).total_seconds() * 1000
            
            # Mark complete
            operation.status = "completed"
            self.total_syncs += 1
            self.successful_syncs += 1
            
            if self.on_sync_complete:
                self.on_sync_complete(operation)
            
            self.completed_operations.append(operation)
            
        except Exception as e:
            operation.status = "failed"
            operation.error = str(e)
            self.failed_syncs += 1
            
            if self.on_sync_error:
                self.on_sync_error(operation)
            
            logger.error(f"Sync failed: {e}")
        
        return operation
    
    async def sync_digital_to_physical(
        self,
        twin_id: str,
        device_id: str,
        commands: Dict[str, Any]
    ) -> SyncOperation:
        """
        Synchronize commands from digital twin to physical device
        
        Args:
            twin_id: Digital twin ID
            device_id: Target physical device ID
            commands: Commands to send
            
        Returns:
            SyncOperation result
        """
        operation = SyncOperation(
            operation_id=f"cmd_{device_id}_{datetime.utcnow().timestamp()}",
            direction="digital_to_physical",
            device_id=device_id,
            twin_id=twin_id,
            data=commands,
            timestamp=datetime.utcnow()
        )
        
        try:
            operation.status = "in_progress"
            start_time = datetime.utcnow()
            
            # Send commands to physical device
            # This would interface with device controller/gateway
            await asyncio.sleep(0.005)  # Simulate command execution time
            
            # Calculate latency
            end_time = datetime.utcnow()
            operation.latency_ms = (end_time - start_time).total_seconds() * 1000
            
            operation.status = "completed"
            self.successful_syncs += 1
            
            logger.info(f"Commands sent to {device_id}: {commands}")
            
        except Exception as e:
            operation.status = "failed"
            operation.error = str(e)
            self.failed_syncs += 1
            logger.error(f"Command failed: {e}")
        
        return operation
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get synchronization statistics"""
        success_rate = (
            self.successful_syncs / self.total_syncs
            if self.total_syncs > 0 else 0.0
        )
        
        # Calculate average latency from recent operations
        recent_ops = list(self.completed_operations)[-100:]
        avg_latency = (
            sum(op.latency_ms for op in recent_ops) / len(recent_ops)
            if recent_ops else 0.0
        )
        
        return {
            "total_syncs": self.total_syncs,
            "successful_syncs": self.successful_syncs,
            "failed_syncs": self.failed_syncs,
            "success_rate": success_rate,
            "avg_latency_ms": avg_latency,
            "pending_operations": len(self.pending_operations),
            "completed_operations": len(self.completed_operations)
        }


# ==========================================
# CONFLICT RESOLUTION
# ==========================================

class ConflictResolver:
    """Resolves conflicts when physical and digital states diverge"""
    
    @staticmethod
    def resolve_last_write_wins(
        physical_state: Dict[str, Any],
        digital_state: Dict[str, Any],
        physical_timestamp: datetime,
        digital_timestamp: datetime
    ) -> Dict[str, Any]:
        """Last write wins strategy"""
        if physical_timestamp > digital_timestamp:
            return physical_state
        else:
            return digital_state
    
    @staticmethod
    def resolve_physical_wins(
        physical_state: Dict[str, Any],
        digital_state: Dict[str, Any],
        physical_timestamp: datetime,
        digital_timestamp: datetime
    ) -> Dict[str, Any]:
        """Physical always wins"""
        return physical_state
    
    @staticmethod
    def resolve_digital_wins(
        physical_state: Dict[str, Any],
        digital_state: Dict[str, Any],
        physical_timestamp: datetime,
        digital_timestamp: datetime
    ) -> Dict[str, Any]:
        """Digital always wins"""
        return digital_state
    
    @staticmethod
    def resolve_merge(
        physical_state: Dict[str, Any],
        digital_state: Dict[str, Any],
        physical_timestamp: datetime,
        digital_timestamp: datetime
    ) -> Dict[str, Any]:
        """Merge both states, preferring newer values per field"""
        merged = digital_state.copy()
        
        # This would need field-level timestamps in practice
        # Simplified: use latest overall timestamp
        if physical_timestamp > digital_timestamp:
            merged.update(physical_state)
        
        return merged
