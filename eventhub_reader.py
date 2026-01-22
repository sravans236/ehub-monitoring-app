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
            
            # Try to get actual message content with aggressive timeout protection
            logger.info(f"Attempting to fetch actual message for partition {partition_id}")
            
            # Try multiple approaches to get the message, starting with fastest
            message_retrieved = False
            
            # Approach 1: Try receive with very short timeout
            try:
                logger.info(f"Trying receive with 180s timeout for partition {partition_id}")
                
                events_received = []
                def on_event_batch(partition_context, events):
                    events_received.extend(events)
                
                with self.client:
                    self.client.receive(
                        on_event=on_event_batch,
                        partition_id=partition_id,
                        starting_position=last_offset,
                        max_batch_size=1,
                        max_wait_time=180  # Very short timeout
                    )
                
                if events_received and len(events_received) > 0:
                    logger.info(f"SUCCESS: Got event with sequence {events_received[0].__dict__.get('sequence_number', 'unknown')}")
                    self._extract_message_data(events_received[0], partition_id)
                    message_retrieved = True
                else:
                    logger.info("No events received in 180s")
                    
            except Exception as batch_error:
                logger.error(f"receive failed: {str(batch_error)}")
            
            # Approach 2: Try with "-1" (latest) position if offset approach failed
            if not message_retrieved:
                try:
                    logger.info(f"Trying receive with latest position (-1) for partition {partition_id}")
                    
                    events_received = []
                    def on_event_batch(partition_context, events):
                        events_received.extend(events)
                    
                    with self.client:
                        self.client.receive(
                            on_event=on_event_batch,
                            partition_id=partition_id,
                            starting_position="-1",  # Latest message
                            max_batch_size=1,
                            max_wait_time=180
                        )
                    
                    if events_received and len(events_received) > 0:
                        logger.info(f"SUCCESS: Got event with sequence {events_received[0].__dict__.get('sequence_number', 'unknown')}")
                        self._extract_message_data(events_received[0], partition_id)
                        message_retrieved = True
                    else:
                        logger.info("No events received with latest position")
                        
                except Exception as latest_error:
                    logger.error(f"Latest position approach failed: {str(latest_error)}")
            
            # Final fallback: Show metadata if message retrieval failed
            if not message_retrieved:
                logger.info("⚠️ Could not retrieve message content in 180s, showing metadata")
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
    
    def get_message_by_offset(self, partition_id: str, offset: str, include_content: bool = True) -> Optional[Dict[str, Any]]:
        """Get message by specific offset"""
        try:
            if not self.client:
                self.connect()
            
            logger.debug(f"Querying partition {partition_id} at offset {offset}")
            
            events_received = []
            def on_event_batch(partition_context, events):
                events_received.extend(events)
            
            with self.client:
                self.client.receive(
                    on_event=on_event_batch,
                    partition_id=partition_id,
                    starting_position=offset,
                    max_batch_size=1,
                    max_wait_time=2
                )
            
            events = events_received
            
            if events:
                event = events[0]
                return self._extract_message_dict(event, partition_id, include_content)
            else:
                logger.info(f"No message found at offset {offset}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting message by offset {offset}: {str(e)}")
            return None
    
    def get_message_by_sequence_number(self, partition_id: str, sequence: int, include_content: bool = True) -> Optional[Dict[str, Any]]:
        """Get message by specific sequence number"""
        try:
            if not self.client:
                self.connect()
            
            logger.debug(f"Querying partition {partition_id} at sequence {sequence}")
            
            # Get partition properties to find offset range
            props = self.get_partition_properties(partition_id)
            if not props or props.get('isEmpty', True):
                logger.info(f"Partition {partition_id} is empty")
                return None
            
            # Use earliest position and scan forward (not ideal but works)
            events_received = []
            def on_event_batch(partition_context, events):
                events_received.extend(events)
            
            with self.client:
                self.client.receive(
                    on_event=on_event_batch,
                    partition_id=partition_id,
                    starting_position="@latest",  # Start from latest and work backwards
                    max_batch_size=10,  # Get a few messages to find the right one
                    max_wait_time=3
                )
            
            events = events_received
            
            # Find message with matching sequence number
            for event in events:
                if event.__dict__.get('sequence_number') == int(sequence):
                    return self._extract_message_dict(event, partition_id, include_content)
            
            logger.info(f"Message with sequence {sequence} not found in recent messages")
            return None
                
        except Exception as e:
            logger.error(f"Error getting message by sequence {sequence}: {str(e)}")
            return None
    
    def _extract_message_dict(self, event, partition_id: str, include_content: bool = True) -> Dict[str, Any]:
        """Extract message data and return as dictionary"""
        try:
            # Get basic event info
            sequence_number = event.__dict__.get('sequence_number', None)
            offset = event.__dict__.get('offset', None)
            enqueued_time = event.__dict__.get('enqueued_time', None)
            partition_key = event.__dict__.get('partition_key', None)
            
            message_data = {
                "topic": self.eventhub_name,
                "partition": partition_id,
                "sequence": sequence_number,
                "offset": offset,
                "enqueuedTime": str(enqueued_time) if enqueued_time else None,
                "partitionKey": partition_key
            }
            
            if include_content:
                # Get message content
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
                
                message_data.update({
                    "messageSize": len(raw_payload_bytes),
                    "content": message_content
                })
            
            return message_data
            
        except Exception as e:
            logger.error(f"Error extracting message dict: {str(e)}")
            return {"error": str(e)}
