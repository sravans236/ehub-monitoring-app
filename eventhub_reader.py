import logging
from typing import Dict, Any, Optional, List
from azure.eventhub import EventHubConsumerClient, EventPosition
from datetime import datetime, timezone
import time

# Import centralized logging config (auto-configures on import)
from .logging_config import configure_logging

logger = logging.getLogger("EventHubReader")


class LatestMessageReader:
    """Reads the latest message from Event Hub partitions"""
    
    def __init__(self, connection_string: str, eventhub_name: str, consumer_group: str = "$Default"):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.client = None
    
    def connect(self):
        """Connect to Event Hub"""
        try:
            self.client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
            logger.info(f"Connected to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub {self.eventhub_name}: {str(e)}")
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
                self.connect()
            
            partition_ids = self.client.get_partition_ids()
            return list(partition_ids)
        except Exception as e:
            logger.error(f"Error getting partition IDs: {str(e)}")
            return []
    
    def get_partition_properties(self, partition_id: str) -> Dict[str, Any]:
        """Get partition properties"""
        try:
            if not self.client:
                self.connect()
            
            props = self.client.get_partition_properties(partition_id)
            
            # Convert props object to dictionary and safely get values
            props_dict = {
                "partition_id": getattr(props, 'partition_id', partition_id),
                "beginning_sequence_number": getattr(props, 'beginning_sequence_number', None),
                "last_enqueued_sequence_number": getattr(props, 'last_enqueued_sequence_number', None),
                "last_enqueued_offset": getattr(props, 'last_enqueued_offset', None),
                "last_enqueued_time_utc": getattr(props, 'last_enqueued_time_utc', None),
                "is_empty": getattr(props, 'is_empty', True)
            }
            
            return {
                "partitionId": partition_id,
                "beginningSequenceNumber": props_dict.get("beginning_sequence_number"),
                "lastEnqueuedSequenceNumber": props_dict.get("last_enqueued_sequence_number"),
                "lastEnqueuedOffset": props_dict.get("last_enqueued_offset"),
                "lastEnqueuedTimeUtc": props_dict.get("last_enqueued_time_utc").isoformat() if props_dict.get("last_enqueued_time_utc") else None,
                "isEmpty": props_dict.get("is_empty", True)
            }
        except Exception as e:
            logger.error(f"Error getting partition properties for {partition_id}: {str(e)}")
            return None
    
    def get_latest_message_batch(self, partition_id: str) -> bool:
        """
        Get the latest message using batch approach with lastEnqueuedOffset
        Much faster than streaming - directly fetches the specific message
        
        Args:
            partition_id: The partition to read from
        
        Returns:
            Dictionary with message details or None if no message found
        """
        try:
            if not self.client:
                self.connect()
            
            # Get partition properties to find the latest offset
            partition_props = self.client.get_partition_properties(partition_id)
            
            if getattr(partition_props, 'is_empty', True):
                logger.debug(f"Partition {partition_id} is empty")
                return None
            
            logger.debug(
                f"Partition {partition_id}: LastSeq={getattr(partition_props, 'last_enqueued_sequence_number', 'unknown')}, "
                f"LastOffset={getattr(partition_props, 'last_enqueued_offset', 'unknown')}"
            )
            
            # Use receive_batch to get exactly 1 message from the last offset
            # This is much faster than streaming
            try:
                events = self.client.receive_batch(
                    partition_id=partition_id,
                    starting_position=EventPosition(offset=getattr(partition_props, 'last_enqueued_offset'), inclusive=True),
                    max_batch_size=1,
                    max_wait_time=3  # Short timeout since we know the message exists
                )
                
                if events:
                    event = events[0]  # Get the single latest message
                    self._extract_message_data(event, partition_id)
                    logger.debug(f"✓ Retrieved latest message from partition {partition_id}")
                    return True  # Return True to indicate success
                else:
                    logger.debug(f"No message retrieved from partition {partition_id} (likely at tail)")
                    return False
                    
            except Exception as batch_error:
                logger.warning(f"Batch retrieval failed for partition {partition_id}: {str(batch_error)}")
                return None
            
        except Exception as e:
            logger.error(f"Error getting latest message from partition {partition_id}: {str(e)}")
            return None
    
    def get_latest_message_by_sequence(self, partition_id: str, partition_props: Dict[str, Any] = None) -> bool:
        """
        Get latest message using lastEnqueuedSequenceNumber
        
        Args:
            partition_id: The partition to read from
            partition_props: Optional partition properties dict to avoid re-fetching
        
        Returns:
            Dictionary with message details or None if no message found
        """
        try:
            if not self.client:
                self.connect()
            
            # Use provided props or fetch them
            if partition_props:
                is_empty = partition_props.get('isEmpty', True)
                last_seq = partition_props.get('lastEnqueuedSequenceNumber')
                
                if is_empty:
                    logger.debug(f"Partition {partition_id} is empty")
                    return None
                    
                logger.debug(f"Using provided props: sequence {last_seq}")
            else:
                # Fallback to fetching props if not provided
                props_obj = self.client.get_partition_properties(partition_id)
                
                if getattr(props_obj, 'is_empty', True):
                    logger.debug(f"Partition {partition_id} is empty")
                    return None
                
                last_seq = getattr(props_obj, 'last_enqueued_sequence_number')
                logger.debug(f"Fetched props: sequence {last_seq}")
            
            # Use sequence number to fetch the exact latest message
            try:
                events = self.client.receive_batch(
                    partition_id=partition_id,
                    starting_position=EventPosition(sequence_number=last_seq, inclusive=True),
                    max_batch_size=1,
                    max_wait_time=3
                )
                
                if events:
                    event = events[0]
                    self._extract_message_data(event, partition_id)
                    logger.debug(f"✓ Retrieved message by sequence from partition {partition_id}")
                    return True  # Return True to indicate success
                else:
                    logger.debug(f"No message found at sequence {last_seq}")
                    return False
                    
            except Exception as batch_error:
                logger.warning(f"Sequence-based retrieval failed for partition {partition_id}: {str(batch_error)}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting message by sequence for partition {partition_id}: {str(e)}")
            return None
    
    def get_latest_message_from_all_partitions_batch(self) -> List[Dict[str, Any]]:
        """
        Get latest message from all partitions using fast batch approach
        Much faster than streaming - completes in seconds instead of minutes
        
        Returns:
            List of message dictionaries, one per partition (if available)
        """
        results = []
        start_time = time.time()
        
        try:
            # Get all partition IDs
            partition_ids = self.get_partition_ids()
            logger.info(f"Found {len(partition_ids)} partitions in Event Hub: {self.eventhub_name}")
            
            if not partition_ids:
                logger.warning(f"No partitions found for EventHub {self.eventhub_name}")
                return results
            
            for partition_id in partition_ids:
                partition_start = time.time()
                
                # Get partition properties first (fast operation)
                partition_props = self.get_partition_properties(partition_id)
                
                if not partition_props:
                    logger.warning(f"Could not get properties for partition {partition_id}")
                    results.append({
                        "eventhubName": self.eventhub_name,
                        "partitionId": partition_id,
                        "status": "error",
                        "partitionProperties": None,
                        "message": None
                    })
                    continue
                
                if partition_props.get("isEmpty", True):
                    logger.debug(f"Partition {partition_id} is empty")
                    results.append({
                        "eventhubName": self.eventhub_name,
                        "partitionId": partition_id,
                        "status": "empty",
                        "partitionProperties": partition_props,
                        "message": None
                    })
                    continue
                
                # Use batch method to get latest message (fast!)
                message = self.get_latest_message_batch(partition_id)
                
                partition_time = time.time() - partition_start
                status = "success" if message else "no_message"
                
                results.append({
                    "eventhubName": self.eventhub_name,
                    "partitionId": partition_id,
                    "status": status,
                    "partitionProperties": partition_props,
                    "message": message,
                    "processingTimeMs": round(partition_time * 1000, 2)
                })
                
                logger.debug(f"Partition {partition_id} processed in {partition_time:.3f}s")
        
        except Exception as e:
            logger.error(f"Error getting latest messages: {str(e)}")
        
        total_time = time.time() - start_time
        logger.info(f"Batch processing completed in {total_time:.2f}s for {len(partition_ids)} partitions")
        return results
    
    def _extract_message_data(self, event, partition_id: str) -> None:
        """Extract and print message data in simple format"""
        try: 
            # Get basic event info
            sequence_number = getattr(event, 'sequence_number', None)
            offset = getattr(event, 'offset', None)
            enqueued_time = getattr(event, 'enqueued_time', None)
            partition_key = getattr(event, 'partition_key', None)
            
            # Get message content as string
            raw_payload_bytes = getattr(event, 'body_as_bytes', lambda: b'')()
            try:
                message_content = raw_payload_bytes.decode('utf-8')
            except UnicodeDecodeError:
                message_content = f"<binary data: {raw_payload_bytes[:100].hex()}>"
            
            # Print message info directly
            logger.info(f"✅ Latest Message Details:")
            logger.info(f"   Topic: {self.eventhub_name}")
            logger.info(f"   Partition: {partition_id}")
            logger.info(f"   Sequence: {sequence_number}")
            logger.info(f"   Offset: {offset}")
            logger.info(f"   EnqueuedTime: {enqueued_time}")
            logger.info(f"   PartitionKey: {partition_key}")
            logger.info(f"   MessageSize: {len(raw_payload_bytes)} bytes")
            logger.info(f"   Content: {message_content}")
            
        except Exception as e:
            logger.error(f"Error extracting message data: {str(e)}")
