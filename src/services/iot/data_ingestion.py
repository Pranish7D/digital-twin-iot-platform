"""
Data Ingestion Pipeline
High-performance data ingestion from IoT devices
Includes validation, transformation, batching, and routing
"""

import asyncio
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from dataclasses import dataclass
from collections import deque
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class DataPoint:
    """Single data point from IoT device"""
    device_id: str
    timestamp: datetime
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    validated: bool = False
    transformed: bool = False


class DataValidator:
    """Validates incoming device data"""
    
    @staticmethod
    def validate(device_id: str, data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate data point
        
        Returns:
            (is_valid, error_message)
        """
        # Check required fields
        if not data:
            return False, "Empty data"
        
        # Check timestamp
        if "timestamp" in data:
            try:
                datetime.fromisoformat(data["timestamp"])
            except:
                return False, "Invalid timestamp format"
        
        # Check for numeric values
        for key, value in data.items():
            if isinstance(value, (int, float)):
                if value != value:  # NaN check
                    return False, f"NaN value in {key}"
                if abs(value) > 1e10:  # Unreasonably large
                    return False, f"Value too large in {key}"
        
        return True, None


class DataTransformer:
    """Transforms/enriches device data"""
    
    @staticmethod
    def transform(device_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data point
        
        Adds:
        - Server-side timestamp
        - Device metadata
        - Computed fields
        """
        transformed = data.copy()
        
        # Add server timestamp
        transformed["server_timestamp"] = datetime.utcnow().isoformat()
        
        # Add device ID if not present
        if "device_id" not in transformed:
            transformed["device_id"] = device_id
        
        # Round numeric values
        for key, value in transformed.items():
            if isinstance(value, float):
                transformed[key] = round(value, 6)
        
        return transformed


class DataIngestionPipeline:
    """
    High-performance data ingestion pipeline
    
    Features:
    - Validation
    - Transformation
    - Batching
    - Async processing
    - Backpressure handling
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        flush_interval_seconds: float = 1.0,
        max_queue_size: int = 10000
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.max_queue_size = max_queue_size
        
        # Processing pipeline
        self.input_queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self.output_queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        
        # Batching
        self.current_batch: List[DataPoint] = []
        
        # Components
        self.validator = DataValidator()
        self.transformer = DataTransformer()
        
        # Callbacks
        self.on_batch_ready: Optional[Callable] = None
        self.on_validation_error: Optional[Callable] = None
        
        # Metrics
        self.total_received = 0
        self.total_validated = 0
        self.total_invalid = 0
        self.total_batches = 0
        
        # State
        self.running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self):
        """Start the pipeline"""
        self.running = True
        
        # Start processing tasks
        self._tasks = [
            asyncio.create_task(self._validation_worker()),
            asyncio.create_task(self._transformation_worker()),
            asyncio.create_task(self._batching_worker())
        ]
        
        logger.info("🚀 Data ingestion pipeline started")
    
    async def stop(self):
        """Stop the pipeline"""
        self.running = False
        
        # Cancel tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Flush remaining data
        if self.current_batch:
            await self._flush_batch()
        
        logger.info("⏹️  Data ingestion pipeline stopped")
    
    async def ingest(
        self,
        device_id: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Ingest data point
        
        Args:
            device_id: Source device ID
            data: Device data
            metadata: Optional metadata
        """
        try:
            data_point = DataPoint(
                device_id=device_id,
                timestamp=datetime.utcnow(),
                data=data,
                metadata=metadata or {}
            )
            
            await self.input_queue.put(data_point)
            self.total_received += 1
            
        except asyncio.QueueFull:
            logger.warning("Input queue full, dropping data point")
    
    async def _validation_worker(self):
        """Worker for data validation"""
        while self.running:
            try:
                # Get data point
                data_point = await self.input_queue.get()
                
                # Validate
                is_valid, error = self.validator.validate(
                    data_point.device_id,
                    data_point.data
                )
                
                if is_valid:
                    data_point.validated = True
                    self.total_validated += 1
                    await self.output_queue.put(data_point)
                else:
                    self.total_invalid += 1
                    logger.warning(f"Invalid data from {data_point.device_id}: {error}")
                    
                    if self.on_validation_error:
                        self.on_validation_error(data_point, error)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Validation error: {e}")
    
    async def _transformation_worker(self):
        """Worker for data transformation"""
        while self.running:
            try:
                # Get validated data point
                data_point = await self.output_queue.get()
                
                # Transform
                data_point.data = self.transformer.transform(
                    data_point.device_id,
                    data_point.data
                )
                data_point.transformed = True
                
                # Add to current batch
                self.current_batch.append(data_point)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Transformation error: {e}")
    
    async def _batching_worker(self):
        """Worker for batch management"""
        while self.running:
            try:
                # Check if batch is ready
                if len(self.current_batch) >= self.batch_size:
                    await self._flush_batch()
                
                # Wait for flush interval
                await asyncio.sleep(self.flush_interval)
                
                # Time-based flush
                if self.current_batch:
                    await self._flush_batch()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batching error: {e}")
    
    async def _flush_batch(self):
        """Flush current batch"""
        if not self.current_batch:
            return
        
        batch = self.current_batch.copy()
        self.current_batch.clear()
        
        self.total_batches += 1
        
        # Call callback
        if self.on_batch_ready:
            await self.on_batch_ready(batch)
        
        logger.debug(f"📦 Batch flushed: {len(batch)} data points")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        return {
            "total_received": self.total_received,
            "total_validated": self.total_validated,
            "total_invalid": self.total_invalid,
            "validation_rate": (
                self.total_validated / self.total_received
                if self.total_received > 0 else 0.0
            ),
            "total_batches": self.total_batches,
            "queue_sizes": {
                "input": self.input_queue.qsize(),
                "output": self.output_queue.qsize()
            },
            "current_batch_size": len(self.current_batch)
        }


# ==========================================
# EXAMPLE USAGE
# ==========================================

async def main():
    """Example: Data ingestion pipeline"""
    
    # Create pipeline
    pipeline = DataIngestionPipeline(
        batch_size=10,
        flush_interval_seconds=2.0
    )
    
    # Setup callback
    async def handle_batch(batch: List[DataPoint]):
        print(f"\n📦 Batch ready with {len(batch)} data points")
        for dp in batch[:3]:  # Show first 3
            print(f"   {dp.device_id}: {dp.data}")
    
    pipeline.on_batch_ready = handle_batch
    
    # Start pipeline
    await pipeline.start()
    
    # Ingest some data
    print("🔄 Ingesting data...")
    for i in range(50):
        await pipeline.ingest(
            device_id=f"device_{i % 5}",
            data={
                "temperature": 20 + i * 0.1,
                "humidity": 50 + i * 0.2,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        await asyncio.sleep(0.1)
    
    # Wait for processing
    await asyncio.sleep(5)
    
    # Show statistics
    stats = pipeline.get_statistics()
    print(f"\n📊 Pipeline Statistics:")
    print(f"   Total received: {stats['total_received']}")
    print(f"   Total validated: {stats['total_validated']}")
    print(f"   Total batches: {stats['total_batches']}")
    print(f"   Validation rate: {stats['validation_rate']*100:.1f}%")
    
    # Stop pipeline
    await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())
