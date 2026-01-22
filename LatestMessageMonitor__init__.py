import logging
import azure.functions as func
from datetime import datetime
import time
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import centralized logging config (auto-configures on import)
from utils.logging_config import configure_logging

from utils.eventhub_reader import LatestMessageReader
from utils. logger import LatestMessageLogger
from utils.eventhub_discovery import discover_eventhubs


def main(timer: func.TimerRequest) -> None:
    """
    Simple EventHub monitor - discover topics, partitions, and print latest messages
    """
    logger = logging.getLogger("LatestMessageMonitor")
    logger.info("🚀 Starting EventHub Discovery and Latest Message Monitor")
    
    # Get connection string
    connection_string = os.environ.get("EventHubConnectionString")
    if not connection_string: 
        logger.error("❌ EventHubConnectionString not configured")
        return
    
    # Optional: For auto-discovery
    subscription_id = os.environ.get("SubscriptionId")
    resource_group = os.environ.get("ResourceGroup")
    eventhub_names_str = os.environ.get("EventHubNames", "")
    fallback_names = [name.strip() for name in eventhub_names_str.split(",") if name.strip()] if eventhub_names_str else None
    
    # ============================================
    # 1. DISCOVER EVENTHUBS (TOPICS)
    # ============================================
    logger.info("\n📡 Discovering EventHubs in namespace...")
    
    eventhub_names = discover_eventhubs(
        connection_string=connection_string,
        subscription_id=subscription_id,
        resource_group=resource_group,
        fallback_names=fallback_names
    )
    
    if not eventhub_names:
        logger.error("❌ No EventHubs discovered")
        return
    
    logger.info(f"✅ Found {len(eventhub_names)} EventHubs: {eventhub_names}")
    
    # ============================================
    # 2. PROCESS EACH EVENTHUB
    # ============================================
    for i, eventhub_name in enumerate(eventhub_names, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 EventHub {i}/{len(eventhub_names)}: {eventhub_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Create reader
            reader = LatestMessageReader(
                connection_string=connection_string,
                eventhub_name=eventhub_name,
                consumer_group="$Default"
            )
            
            reader.connect()
            
            # ============================================
            # 3. DISCOVER PARTITIONS FOR THIS EVENTHUB
            # ============================================
            partition_ids = reader.get_partition_ids()
            logger.info(f"📋 Found {len(partition_ids)} partitions: {partition_ids}")
            
            # ============================================
            # 4. PROCESS EACH PARTITION
            # ============================================
            for partition_id in partition_ids:
                logger.info(f"\n  🔹 Partition: {partition_id}")
                
                # Get partition properties
                props = reader.get_partition_properties(partition_id)
                if not props:
                    logger.error(f"    ❌ Could not get properties for partition {partition_id}")
                    continue
                
                # Print partition properties
                logger.info(f"    📋 Properties:")
                logger.info(f"       • IsEmpty: {props.get('isEmpty', True)}")
                logger.info(f"       • BeginSeqNum: {props.get('beginningSequenceNumber', 'N/A')}")
                logger.info(f"       • LastSeqNum: {props.get('lastEnqueuedSequenceNumber', 'N/A')}")
                logger.info(f"       • LastOffset: {props.get('lastEnqueuedOffset', 'N/A')}")
                logger.info(f"       • LastTime: {props.get('lastEnqueuedTimeUtc', 'N/A')}")
                
                # ============================================
                # 5. GET LATEST MESSAGE FROM LATEST SEQUENCE NUMBER
                # ============================================
                if props.get('isEmpty', True):
                    logger.info(f"    📭 No messages in partition")
                else:
                    logger.info(f"    🎯 Getting latest message from sequence {props.get('lastEnqueuedSequenceNumber', 'N/A')}...")
                    
                    # Use sequence number to get latest message (message details are printed directly)
                    success = reader.get_latest_message_by_sequence(partition_id, props)
                    
                    if not success:
                        logger.info(f"    ❌ Could not retrieve latest message")
            
            reader.disconnect()
            logger.info(f"✅ Completed EventHub: {eventhub_name}")
            
        except Exception as e:
            logger.error(f"❌ Error processing EventHub {eventhub_name}: {str(e)}")
    
    logger.info(f"\n🏁 Monitor completed for {len(eventhub_names)} EventHubs")
