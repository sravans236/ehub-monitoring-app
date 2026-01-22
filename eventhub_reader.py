import logging
from typing import Dict, Any, Optional, List
from azure.eventhub import EventHubConsumerClient
try:
    from azure.eventhub import EventPosition
except ImportError:
    # For newer SDK versions, use string positions
    EventPosition = None
from datetime import datetime, timezone
import time
import json

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
            
            return {
                "partitionId": partition_id,
                "beginningSequenceNumber": props.get("beginning_sequence_number"),
                "lastEnqueuedSequenceNumber": props.get("last_enqueued_sequence_number"),
                "lastEnqueuedOffset": props.get("last_enqueued_offset"),
                "lastEnqueuedTimeUtc": props.get("last_enqueued_time_utc").isoformat() if props.get("last_enqueued_time_utc") else None,
                "isEmpty": props.get("is_empty", True)
            }
        except Exception as e:
            logger.error(f"Error getting partition properties for {partition_id}: {str(e)}")
            return None
    
    def get_latest_message_by_sequence(self, partition_id: str, partition_props: Dict[str, Any] = None) -> bool:
        """
        Get latest message using sequence number - direct and fast approach
        """
        try:
            if not self.client:
                self.connect()
            
            # Use provided props or fetch them
            if partition_props:
                is_empty = partition_props.get('isEmpty', True)
                last_seq = partition_props.get('lastEnqueuedSequenceNumber')
                last_offset = partition_props.get('lastEnqueuedOffset')
                
                if is_empty:
                    logger.debug(f"Partition {partition_id} is empty")
                    return False
                    
                logger.debug(f"Using provided props: sequence {last_seq}, offset {last_offset}")
            else:
                # Fallback to fetching props if not provided
                props_dict = self.get_partition_properties(partition_id)
                
                if not props_dict or props_dict.get('isEmpty', True):
                    logger.debug(f"Partition {partition_id} is empty")
                    return False
                
                last_seq = props_dict.get('lastEnqueuedSequenceNumber')
                last_offset = props_dict.get('lastEnqueuedOffset')
                logger.debug(f"Fetched props: sequence {last_seq}, offset {last_offset}")
            
            # Direct approach: Use the simple receive method instead of receive_batch
            logger.debug(f"Creating consumer for partition {partition_id} from offset {last_offset}")
            
            # Create partition consumer for direct access
            from azure.eventhub._consumer import PartitionConsumer
            
            # Use the offset approach which is more reliable than sequence number
            try:
                # Create consumer starting from the last offset
                consumer = self.client._create_consumer(
                    consumer_group=self.consumer_group,
                    partition_id=partition_id,
                    event_position=last_offset,  # Use offset directly - more reliable
                    owner_level=None
                )
                
                # Receive with short timeout - should be immediate since we know message exists
                event_found = False
                for event in consumer.receive(max_wait_time=2, max_batch_size=1):
                    logger.debug(f"Got event with sequence {event.__dict__.get('sequence_number', 'unknown')}")
                    self._extract_message_data(event, partition_id)
                    event_found = True
                    break  # Only need the first (latest) one
                
                consumer.close()
                
                if event_found:
                    return True
                else:
                    logger.info(f"⚠️ No event received in 2s for partition {partition_id}")
                    # Fall back to showing metadata
                    self._show_metadata(partition_id, last_seq, last_offset)
                    return True
                    
            except ImportError:
                logger.debug("PartitionConsumer not available, using alternative approach")
                # Alternative: just show the metadata since we have it
                self._show_metadata(partition_id, last_seq, last_offset)
                return True
                
            except Exception as consumer_error:
                logger.warning(f"Consumer approach failed: {str(consumer_error)}")
                # Fall back to showing metadata
                self._show_metadata(partition_id, last_seq, last_offset)
                return True
                
        except Exception as e:
            logger.error(f"Error getting message by sequence for partition {partition_id}: {str(e)}")
            return False
    
    def _show_metadata(self, partition_id: str, sequence_number, offset):
        """Show message metadata when content can't be retrieved"""
        message_metadata = {
            "topic": self.eventhub_name,
            "partition": partition_id,
            "sequence": sequence_number,
            "offset": offset,
            "status": "Message exists at this location (content retrieval issue)"
        }
        logger.info(f"📋 Message Metadata: {json.dumps(message_metadata, indent=2)}")
    
    def _extract_message_data(self, event, partition_id: str) -> None:
        """Extract and print message data in simple format"""
        try: 
            # Get basic event info
            sequence_number = event.__dict__.get('sequence_number', None)
            offset = event.__dict__.get('offset', None)
            enqueued_time = event.__dict__.get('enqueued_time', None)
            partition_key = event.__dict__.get('partition_key', None)
            
            # Get message content as string
            try:
                if hasattr(event, 'body_as_bytes'):
                    raw_payload_bytes = event.body_as_bytes()
                else:
                    raw_payload_bytes = event.__dict__.get('body', b'')
            except:
                raw_payload_bytes = b''
            try:
                message_content = raw_payload_bytes.decode('utf-8')
            except UnicodeDecodeError:
                message_content = f"<binary data: {raw_payload_bytes[:100].hex()}>"
            
            # Print message info directly
            message_data = {
                "topic": self.eventhub_name,
                "partition": partition_id,
                "sequence": sequence_number,
                "offset": offset,
                "enqueuedTime": str(enqueued_time) if enqueued_time else None,
                "partitionKey": partition_key,
                "messageSize": len(raw_payload_bytes),
                "content": message_content
            }
            
            logger.info(f"✅ Latest Message Details: {json.dumps(message_data)}")
            
        except Exception as e:
            logger.error(f"Error extracting message data: {str(e)}")
