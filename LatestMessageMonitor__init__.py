import logging
import azure.functions as func
from datetime import datetime
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.eventhub_reader import EventHubStreamReader  # Changed from LatestMessageReader
from utils.logger import LatestMessageLogger
from utils.eventhub_discovery import discover_eventhubs


def main(timer: func.TimerRequest) -> None:
    """
    Timer-triggered function that runs every 5 minutes
    Auto-discovers all Event Hubs in the namespace and fetches latest messages
    """
    start_time = time.time()
    
    # Initialize logger
    logger = logging.getLogger("LatestMessageMonitor")
    monitor_logger = LatestMessageLogger(logger)
    
    utc_timestamp = datetime.utcnow().isoformat()
    
    if timer.past_due:
        logger.info('The timer is past due!')
    
    logger.info(f"Latest Message Monitor started at {utc_timestamp}")
    
    # ============================================
    # CONFIGURATION
    # ============================================
    
    connection_string = os.environ. get("EventHubConnectionString")
    
    if not connection_string: 
        logger.error("EventHubConnectionString not configured")
        return
    
    # Optional: For auto-discovery via Management API
    subscription_id = os.environ.get("SubscriptionId")
    resource_group = os.environ.get("ResourceGroup")
    
    # Optional:  Fallback list of Event Hub names
    eventhub_names_str = os.environ.get("EventHubNames", "")
    fallback_names = [name.strip() for name in eventhub_names_str.split(",") if name.strip()] if eventhub_names_str else None
    
    # Lookback time in seconds (how far back to search for messages)
    lookback_seconds = int(os.environ.get("LookbackSeconds", "300"))  # Default:  5 minutes
    
    # ============================================
    # AUTO-DISCOVER EVENT HUBS
    # ============================================
    
    logger.info("\n" + "="*60)
    logger.info("DISCOVERING EVENT HUBS IN NAMESPACE")
    logger.info("="*60)
    
    eventhub_names = discover_eventhubs(
        connection_string=connection_string,
        subscription_id=subscription_id,
        resource_group=resource_group,
        fallback_names=fallback_names
    )
    
    if not eventhub_names:
        logger.error("No Event Hubs discovered.  Exiting.")
        logger.error("Please configure one of the following:")
        logger.error("  1. SubscriptionId + ResourceGroup (for auto-discovery)")
        logger.error("  2. EventHubNames (comma-separated fallback list)")
        return
    
    logger.info(f"\n✓ Discovered {len(eventhub_names)} Event Hubs:")
    for eh_name in eventhub_names:
        logger.info(f"  - {eh_name}")
    
    # ============================================
    # PROCESS EACH EVENT HUB
    # ============================================
    
    all_eventhubs_results = {}
    
    for eventhub_name in eventhub_names:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Event Hub: {eventhub_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Create reader for this Event Hub (using StreamReader)
            reader = EventHubStreamReader(
                connection_string=connection_string,
                eventhub_name=eventhub_name,
                consumer_group="$Default"
            )
            
            # Connect to Event Hub
            reader.connect()
            
            # Get latest messages from all partitions
            partition_results = reader.get_latest_from_all_partitions(lookback_seconds=lookback_seconds)
            
            # Store results
            all_eventhubs_results[eventhub_name] = partition_results
            
            # Log each partition's latest message
            for result in partition_results:
                partition_id = result. get("partitionId")
                status = result.get("status")
                message = result.get("message")
                partition_props = result.get("partitionProperties")
                
                logger.info(f"\nPartition {partition_id} Status: {status}")
                
                if partition_props:
                    logger. info(
                        f"  Partition Properties: "
                        f"LastSeq={partition_props['lastEnqueuedSequenceNumber']}, "
                        f"LastOffset={partition_props['lastEnqueuedOffset']}, "
                        f"LastTime={partition_props['lastEnqueuedTimeUtc']}, "
                        f"IsEmpty={partition_props['isEmpty']}"
                    )
                
                if message:
                    # Log the latest message
                    monitor_logger.log_latest_message(message)
                    
                    logger.info(
                        f"  Latest Message:  "
                        f"SeqNum={message['sequenceNumber']}, "
                        f"Offset={message['offset']}, "
                        f"EnqueuedTime={message['enqueuedTime']}, "
                        f"Size={message['payload']['sizeBytes']} bytes"
                    )
                    logger.info(f"  Payload Preview: {message['payload']['truncated'][:200]}")
                else:
                    logger.info(f"  No message available in last {lookback_seconds} seconds")
            
            # Log Event Hub summary
            monitor_logger.log_eventhub_summary(eventhub_name, partition_results)
            
            # Disconnect
            reader.disconnect()
        
        except Exception as e: 
            logger.error(f"Error processing Event Hub {eventhub_name}: {str(e)}", exc_info=True)
            all_eventhubs_results[eventhub_name] = [{
                "error": str(e),
                "eventhubName": eventhub_name
            }]
    
    # ============================================
    # SUMMARY
    # ============================================
    
    # Calculate execution time
    execution_time_ms = (time.time() - start_time) * 1000
    
    # Log overall summary
    monitor_logger.log_monitor_run_summary(all_eventhubs_results, execution_time_ms)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Monitor run completed in {execution_time_ms:.2f}ms")
    logger.info(f"Processed {len(eventhub_names)} Event Hubs")
    logger.info(f"{'='*60}")
