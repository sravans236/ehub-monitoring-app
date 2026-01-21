import logging
import json
from typing import Dict, Any, List, Optional
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime
import uuid

logger = logging.getLogger("EventHubProducer")


class EventHubProducer:
    """Producer class to send events to Event Hub"""
    
    def __init__(self, connection_string: str, eventhub_name: str):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.producer = None
    
    def connect(self):
        """Connect to Event Hub"""
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            logger. info(f"Connected to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub:  {str(e)}")
            raise
    
    def disconnect(self):
        """Disconnect from Event Hub"""
        if self. producer:
            self.producer. close()
            logger.info(f"Disconnected from Event Hub: {self.eventhub_name}")
    
    def send_single_event(
        self,
        payload: Dict[str, Any],
        partition_key: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send a single event to Event Hub
        
        Args: 
            payload: The data to send (will be JSON serialized)
            partition_key:  Optional partition key for routing
            properties: Optional custom properties/headers
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.producer:
                self.connect()
            
            # Create event data
            event_data = EventData(json.dumps(payload))
            
            # Add custom properties if provided
            if properties: 
                for key, value in properties.items():
                    event_data.properties[key] = value
            
            # Send event
            if partition_key:
                # Send to specific partition based on key
                self.producer. send_event(event_data, partition_key=partition_key)
                logger.info(f"Sent event with partition key: {partition_key}")
            else:
                # Round-robin across partitions
                self.producer. send_event(event_data)
                logger.info(f"Sent event (round-robin)")
            
            return True
        
        except Exception as e: 
            logger.error(f"Error sending event: {str(e)}", exc_info=True)
            return False
    
    def send_batch_events(
        self,
        payloads: List[Dict[str, Any]],
        partition_key: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Send multiple events in a batch
        
        Args:
            payloads: List of data dictionaries to send
            partition_key: Optional partition key for routing all events
            properties: Optional custom properties for all events
        
        Returns: 
            Dictionary with success count and details
        """
        try: 
            if not self.producer:
                self.connect()
            
            # Create event batch
            if partition_key:
                event_batch = self.producer.create_batch(partition_key=partition_key)
            else:
                event_batch = self.producer.create_batch()
            
            sent_count = 0
            failed_count = 0
            
            for payload in payloads: 
                try:
                    # Create event data
                    event_data = EventData(json.dumps(payload))
                    
                    # Add custom properties if provided
                    if properties:
                        for key, value in properties.items():
                            event_data.properties[key] = value
                    
                    # Try to add to batch
                    event_batch.add(event_data)
                    sent_count += 1
                
                except ValueError: 
                    # Batch is full, send it and create a new batch
                    self.producer.send_batch(event_batch)
                    logger.info(f"Sent batch of {sent_count} events")
                    
                    # Create new batch
                    if partition_key:
                        event_batch = self.producer.create_batch(partition_key=partition_key)
                    else: 
                        event_batch = self.producer.create_batch()
                    
                    # Add the current event to new batch
                    event_data = EventData(json.dumps(payload))
                    if properties:
                        for key, value in properties.items():
                            event_data.properties[key] = value
                    event_batch.add(event_data)
                    sent_count += 1
                
                except Exception as e:
                    logger.error(f"Error adding event to batch: {str(e)}")
                    failed_count += 1
            
            # Send remaining events in batch
            if len(event_batch) > 0:
                self.producer.send_batch(event_batch)
                logger.info(f"Sent final batch of {len(event_batch)} events")
            
            result = {
                "success":  True,
                "totalEvents": len(payloads),
                "sentCount": sent_count,
                "failedCount": failed_count,
                "partitionKey": partition_key
            }
            
            logger.info(f"Batch send complete: {sent_count} sent, {failed_count} failed")
            return result
        
        except Exception as e: 
            logger.error(f"Error sending batch: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "totalEvents": len(payloads),
                "sentCount": 0,
                "failedCount": len(payloads)
            }
    
    def send_to_partition(
        self,
        payload: Dict[str, Any],
        partition_id: str,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send event to a specific partition ID
        
        Args:
            payload: The data to send
            partition_id: Target partition ID (e.g., "0", "1", "2")
            properties: Optional custom properties
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self. producer:
                self.connect()
            
            # Create event batch for specific partition
            event_batch = self.producer.create_batch(partition_id=partition_id)
            
            # Create event data
            event_data = EventData(json.dumps(payload))
            
            # Add custom properties if provided
            if properties: 
                for key, value in properties.items():
                    event_data.properties[key] = value
            
            # Add to batch and send
            event_batch.add(event_data)
            self.producer.send_batch(event_batch)
            
            logger.info(f"Sent event to partition {partition_id}")
            return True
        
        except Exception as e: 
            logger.error(f"Error sending to partition {partition_id}:  {str(e)}", exc_info=True)
            return False


def generate_sample_payload(event_type: str = "default") -> Dict[str, Any]:
    """Generate sample payload for testing"""
    
    base_payload = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "eventType": event_type,
        "source": "azure-function-producer"
    }
    
    if event_type == "order":
        base_payload. update({
            "orderId": f"ORD-{uuid.uuid4().hex[:8]. upper()}",
            "customerId": f"CUST-{uuid.uuid4().hex[:6].upper()}",
            "amount": round(50 + (100 * hash(str(uuid.uuid4())) % 100), 2),
            "currency": "USD",
            "status": "pending"
        })
    elif event_type == "sensor":
        base_payload.update({
            "sensorId":  f"SENSOR-{uuid. uuid4().hex[:4].upper()}",
            "temperature": round(20 + (15 * hash(str(uuid. uuid4())) % 100 / 100), 2),
            "humidity": round(40 + (30 * hash(str(uuid. uuid4())) % 100 / 100), 2),
            "pressure": round(1000 + (50 * hash(str(uuid. uuid4())) % 100 / 100), 2)
        })
    elif event_type == "log":
        base_payload.update({
            "level": ["INFO", "WARNING", "ERROR"][hash(str(uuid.uuid4())) % 3],
            "message": f"Sample log message {uuid.uuid4().hex[:8]}",
            "service": "sample-service",
            "traceId": str(uuid.uuid4())
        })
    else:
        base_payload.update({
            "data": {
                "value": hash(str(uuid.uuid4())) % 1000,
                "description": "Sample event data"
            }
        })
    
    return base_payload