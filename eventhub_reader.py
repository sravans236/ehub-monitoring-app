import logging
from typing import Dict, Any, Optional, List
from azure.eventhub import EventHubConsumerClient, EventPosition
from datetime import datetime, timedelta
import time

logger = logging.getLogger("EventHubReader")


class LatestMessageReader:
    """Reads the latest message from Event Hub partitions"""
    
    def __init__(self, connection_string: str, eventhub_name: str, consumer_group: str = "$Default"):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self. consumer_group = consumer_group
        self.client = None
    
    def connect(self):
        """Connect to Event Hub"""
        try:
            self. client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
            logger. info(f"Connected to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub {self.eventhub_name}:  {str(e)}")
            raise
    
    def disconnect(self):
        """Disconnect from Event Hub"""
        if self.client:
            self.client.close()
            logger.info(f"Disconnected from Event Hub: {self.eventhub_name}")
    
    def get_partition_ids(self) -> List[str]:
        """Get all partition IDs for the Event Hub"""
        try: 
            if not self.client:
                self. connect()
            
            partition_ids = self.client.get_partition_ids()
            return list(partition_ids)
        except Exception as e:
            logger. error(f"Error getting partition IDs: {str(e)}")
            return []
    
    def get_partition_properties(self, partition_id: str) -> Dict[str, Any]:
        """Get partition properties"""
        try:
            if not self.client:
                self.connect()
            
            props = self.client.get_partition_properties(partition_id)
            
            return {
                "partitionId": partition_id,
                "beginningSequenceNumber": props.beginning_sequence_number,
                "lastEnqueuedSequenceNumber": props.last_enqueued_sequence_number,
                "lastEnqueuedOffset": props.last_enqueued_offset,
                "lastEnqueuedTimeUtc": props.last_enqueued_time_utc. isoformat() if props.last_enqueued_time_utc else None,
                "isEmpty": props.is_empty
            }
        except Exception as e:
            logger.error(f"Error getting partition properties for {partition_id}: {str(e)}")
            return None
    
    def get_latest_message(self, partition_id: str, timeout_seconds: int = 10) -> Optional[Dict[str, Any]]:
        """
        Get the latest message from a specific partition
        
        Args: 
            partition_id: The partition to read from
            timeout_seconds: Maximum time to wait for a message
        
        Returns:
            Dictionary with message details or None if no message found
        """
        try:
            if not self.client:
                self.connect()
            
            # First, get partition properties to find the latest offset
            partition_props = self. client.get_partition_properties(partition_id)
            
            if partition_props. is_empty:
                logger. info(f"Partition {partition_id} is empty")
                return None
            
            # Start reading from the last enqueued sequence number
            latest_message = None
            message_found = False
            
            # Create a receiver starting from the latest offset
            # We use offset = last_enqueued_offset to get the latest message
            with self.client:
                # Read from the last offset (exclusive=False means include this offset)
                partition_client = self.client._create_consumer(
                    consumer_group=self.consumer_group,
                    partition_id=partition_id,
                    event_position=EventPosition(offset=partition_props.last_enqueued_offset, inclusive=True)
                )
                
                start_time = time.time()
                
                # Receive events (should get the latest one)
                for event in partition_client.receive(max_wait_time=timeout_seconds, max_batch_size=1):
                    # Extract all metadata
                    message_data = self._extract_message_data(event, partition_id)
                    latest_message = message_data
                    message_found = True
                    break  # We only want the latest one
                    
                    # Check timeout
                    if time.time() - start_time > timeout_seconds:
                        break
                
                partition_client.close()
            
            if not message_found:
                logger.info(f"No message found in partition {partition_id} within timeout")
                return None
            
            return latest_message
        
        except Exception as e:
            logger.error(f"Error getting latest message from partition {partition_id}: {str(e)}", exc_info=True)
            return None
    
    def get_latest_message_from_all_partitions(self, timeout_per_partition: int = 10) -> List[Dict[str, Any]]:
        """
        Get the latest message from all partitions
        
        Returns:
            List of message dictionaries, one per partition (if available)
        """
        results = []
        
        try:
            # Get all partition IDs
            partition_ids = self.get_partition_ids()
            logger.info(f"Found {len(partition_ids)} partitions in Event Hub:  {self.eventhub_name}")
            
            for partition_id in partition_ids:
                logger.info(f"Reading latest message from partition {partition_id}...")
                
                # Get partition properties first
                partition_props = self. get_partition_properties(partition_id)
                
                if partition_props and partition_props["isEmpty"]:
                    logger.info(f"Partition {partition_id} is empty - no messages to read")
                    results.append({
                        "eventhubName": self.eventhub_name,
                        "partitionId": partition_id,
                        "status": "empty",
                        "partitionProperties": partition_props,
                        "message": None
                    })
                    continue
                
                # Get the latest message
                message = self.get_latest_message(partition_id, timeout_per_partition)
                
                results.append({
                    "eventhubName": self.eventhub_name,
                    "partitionId": partition_id,
                    "status": "success" if message else "no_message",
                    "partitionProperties": partition_props,
                    "message": message
                })
        
        except Exception as e: 
            logger.error(f"Error getting latest messages:  {str(e)}", exc_info=True)
        
        return results
    
    def _extract_message_data(self, event, partition_id:  str) -> Dict[str, Any]:
        """Extract all metadata and payload from an event"""
        try: 
            # Get raw payload
            raw_payload_bytes = event.body_as_bytes()
            payload_size = len(raw_payload_bytes)
            
            # Decode and truncate payload
            try:
                raw_payload = raw_payload_bytes.decode('utf-8')
                payload_truncated = raw_payload[:1000] + ("..." if len(raw_payload) > 1000 else "")
            except UnicodeDecodeError:
                payload_truncated = f"<binary data:  {raw_payload_bytes[: 100]. hex()}...>"
            
            # Extract system properties
            system_properties = {}
            if hasattr(event, 'system_properties') and event.system_properties:
                system_properties = dict(event.system_properties)
            
            # Extract user properties
            user_properties = {}
            if hasattr(event, 'properties') and event.properties:
                user_properties = dict(event. properties)
            
            message_data = {
                "eventhubName": self.eventhub_name,
                "partitionId": partition_id,
                "sequenceNumber": event. sequence_number,
                "offset": event.offset,
                "enqueuedTime": event.enqueued_time.isoformat() if event.enqueued_time else None,
                "partitionKey": event.partition_key,
                "retrievedAt": datetime.utcnow().isoformat(),
                "payload": {
                    "truncated": payload_truncated,
                    "sizeBytes": payload_size
                },
                "systemProperties": {
                    "correlationId": system_properties.get('correlation-id'),
                    "messageId": system_properties.get('message-id'),
                    "contentType": system_properties.get('content-type'),
                    "contentEncoding": system_properties. get('content-encoding'),
                    "all": system_properties
                },
                "userProperties": user_properties
            }
            
            return message_data
        
        except Exception as e:
            logger.error(f"Error extracting message data:  {str(e)}")
            return {
                "error": str(e),
                "partitionId": partition_id
            }