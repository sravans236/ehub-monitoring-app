import logging
from typing import Dict, Any, Optional, List
from azure.eventhub import EventHubConsumerClient
from datetime import datetime, timedelta
import time

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
            self. client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
            logger. info(f"Connected to Event Hub:  {self.eventhub_name}")
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
                self.connect()
            
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
            
            # Get partition properties to check if empty
            partition_props = self. client.get_partition_properties(partition_id)
            
            if partition_props. is_empty:
                logger. info(f"Partition {partition_id} is empty")
                return None
            
            # Store the latest message
            latest_message = None
            message_received = False
            
            def on_event(partition_context, event):
                """Callback to receive events"""
                nonlocal latest_message, message_received
                
                if event: 
                    latest_message = self._extract_message_data(event, partition_id)
                    message_received = True
            
            def on_error(partition_context, error):
                """Callback for errors"""
                logger.error(f"Error receiving from partition {partition_id}: {error}")
            
            # Receive from the latest position (last message)
            # starting_position="-1" means start from the end (latest)
            with self.client:
                # Create receiver that starts from the latest offset
                self.client.receive(
                    on_event=on_event,
                    on_error=on_error,
                    partition_id=partition_id,
                    starting_position="-1",  # Start from end (latest message)
                    max_wait_time=timeout_seconds
                )
            
            if not message_received:
                logger. info(f"No new messages received from partition {partition_id} within timeout")
                # If no new message, try to get from the last offset
                return self._get_message_at_offset(partition_id, partition_props.last_enqueued_offset)
            
            return latest_message
        
        except Exception as e: 
            logger.error(f"Error getting latest message from partition {partition_id}: {str(e)}", exc_info=True)
            return None
    
    def _get_message_at_offset(self, partition_id: str, offset: str, timeout_seconds: int = 5) -> Optional[Dict[str, Any]]:
        """
        Get message at a specific offset
        
        Args: 
            partition_id: The partition ID
            offset: The offset to read from
            timeout_seconds:  Timeout for reading
        
        Returns:
            Message data or None
        """
        try:
            message_data = None
            
            def on_event(partition_context, event):
                nonlocal message_data
                if event:
                    message_data = self._extract_message_data(event, partition_id)
            
            def on_error(partition_context, error):
                logger.debug(f"Error reading at offset {offset}: {error}")
            
            with self.client:
                self. client.receive(
                    on_event=on_event,
                    on_error=on_error,
                    partition_id=partition_id,
                    starting_position=offset,  # Start from specific offset
                    max_wait_time=timeout_seconds
                )
            
            return message_data
        
        except Exception as e:
            logger.debug(f"Could not read message at offset {offset}:  {str(e)}")
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
    
    def _extract_message_data(self, event, partition_id: str) -> Dict[str, Any]:
        """Extract all metadata and payload from an event"""
        try:
            # Get raw payload
            raw_payload_bytes = event.body_as_bytes()
            payload_size = len(raw_payload_bytes)
            
            # Decode and truncate payload
            try:
                raw_payload = raw_payload_bytes. decode('utf-8')
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
                "sequenceNumber": event.sequence_number,
                "offset": event. offset,
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
                    "contentEncoding": system_properties.get('content-encoding'),
                    "all": system_properties
                },
                "userProperties": user_properties
            }
            
            return message_data
        
        except Exception as e:
            logger. error(f"Error extracting message data: {str(e)}")
            return {
                "error": str(e),
                "partitionId": partition_id
            }


class EventHubStreamReader:
    """
    Alternative reader that reads recent messages (last N seconds)
    More reliable than trying to get exactly the latest message
    """
    
    def __init__(self, connection_string: str, eventhub_name: str, consumer_group:  str = "$Default"):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.client = None
    
    def connect(self):
        """Connect to Event Hub"""
        try:
            self.client = EventHubConsumerClient. from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self. eventhub_name
            )
            logger.info(f"Connected to Event Hub: {self. eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub {self.eventhub_name}: {str(e)}")
            raise
    
    def disconnect(self):
        """Disconnect from Event Hub"""
        if self.client:
            self.client.close()
            logger.info(f"Disconnected from Event Hub: {self. eventhub_name}")
    
    def get_recent_messages(self, partition_id: str, lookback_seconds: int = 300, max_messages: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent messages from a partition (last N seconds)
        
        Args:
            partition_id:  Partition to read from
            lookback_seconds: How many seconds to look back (default: 5 minutes)
            max_messages: Maximum messages to retrieve
        
        Returns:
            List of messages (newest first)
        """
        try:
            if not self.client:
                self.connect()
            
            messages = []
            
            # Calculate starting time
            starting_time = datetime.utcnow() - timedelta(seconds=lookback_seconds)
            
            def on_event(partition_context, event):
                if event:
                    # Extract message data
                    raw_payload_bytes = event.body_as_bytes()
                    
                    try:
                        raw_payload = raw_payload_bytes.decode('utf-8')
                        payload_truncated = raw_payload[:1000] + ("..." if len(raw_payload) > 1000 else "")
                    except UnicodeDecodeError:
                        payload_truncated = f"<binary data: {raw_payload_bytes[: 100].hex()}...>"
                    
                    system_properties = dict(event.system_properties) if event.system_properties else {}
                    user_properties = dict(event.properties) if event.properties else {}
                    
                    message_data = {
                        "eventhubName": self.eventhub_name,
                        "partitionId": partition_id,
                        "sequenceNumber": event.sequence_number,
                        "offset": event.offset,
                        "enqueuedTime": event.enqueued_time.isoformat() if event.enqueued_time else None,
                        "partitionKey": event.partition_key,
                        "retrievedAt": datetime.utcnow().isoformat(),
                        "payload":  {
                            "truncated":  payload_truncated,
                            "sizeBytes": len(raw_payload_bytes)
                        },
                        "systemProperties":  {
                            "correlationId": system_properties.get('correlation-id'),
                            "messageId": system_properties.get('message-id'),
                            "contentType": system_properties.get('content-type'),
                            "all": system_properties
                        },
                        "userProperties": user_properties
                    }
                    
                    messages.append(message_data)
                    
                    # Stop if we have enough messages
                    if len(messages) >= max_messages:
                        return
            
            def on_error(partition_context, error):
                logger.error(f"Error receiving from partition {partition_id}: {error}")
            
            # Receive messages starting from the specified time
            with self.client:
                self.client.receive(
                    on_event=on_event,
                    on_error=on_error,
                    partition_id=partition_id,
                    starting_position=starting_time.isoformat(),  # Start from timestamp
                    max_wait_time=10
                )
            
            # Sort by sequence number (descending) to get latest first
            messages.sort(key=lambda x: x["sequenceNumber"], reverse=True)
            
            logger.info(f"Retrieved {len(messages)} recent messages from partition {partition_id}")
            return messages
        
        except Exception as e: 
            logger.error(f"Error getting recent messages from partition {partition_id}: {str(e)}", exc_info=True)
            return []
    
    def get_latest_from_partition(self, partition_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the single latest message from a partition
        
        Args:
            partition_id: Partition to read from
        
        Returns:
            Latest message or None
        """
        messages = self.get_recent_messages(partition_id, lookback_seconds=600, max_messages=1)
        return messages[0] if messages else None
    
    def get_latest_from_all_partitions(self, lookback_seconds: int = 300) -> List[Dict[str, Any]]:
        """
        Get latest message from all partitions
        
        Args:
            lookback_seconds:  How many seconds to look back
        
        Returns:
            List of results per partition
        """
        results = []
        
        try: 
            if not self.client:
                self.connect()
            
            # Get all partition IDs
            partition_ids = self.client.get_partition_ids()
            logger.info(f"Found {len(partition_ids)} partitions in Event Hub: {self. eventhub_name}")
            
            for partition_id in partition_ids:
                logger.info(f"Reading latest message from partition {partition_id}...")
                
                # Get partition properties
                partition_props = self.client.get_partition_properties(partition_id)
                
                partition_info = {
                    "partitionId": partition_id,
                    "beginningSequenceNumber": partition_props.beginning_sequence_number,
                    "lastEnqueuedSequenceNumber": partition_props. last_enqueued_sequence_number,
                    "lastEnqueuedOffset": partition_props.last_enqueued_offset,
                    "lastEnqueuedTimeUtc": partition_props.last_enqueued_time_utc.isoformat() if partition_props.last_enqueued_time_utc else None,
                    "isEmpty": partition_props.is_empty
                }
                
                if partition_props.is_empty:
                    logger.info(f"Partition {partition_id} is empty")
                    results.append({
                        "eventhubName": self.eventhub_name,
                        "partitionId": partition_id,
                        "status": "empty",
                        "partitionProperties": partition_info,
                        "message":  None
                    })
                    continue
                
                # Get latest message
                message = self.get_latest_from_partition(partition_id)
                
                results.append({
                    "eventhubName": self.eventhub_name,
                    "partitionId": partition_id,
                    "status": "success" if message else "no_message",
                    "partitionProperties": partition_info,
                    "message": message
                })
        
        except Exception as e: 
            logger.error(f"Error getting latest messages from all partitions: {str(e)}", exc_info=True)
        
        return results
