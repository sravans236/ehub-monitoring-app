import logging
import json
import os
import time
import azure.functions as func
from typing import Dict, Any
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.eventhub_reader import LatestMessageReader
from utils.logging_config import configure_logging

logger = logging.getLogger("EventHubQueryFunction")

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to query EventHub messages by offset/sequence + partition
    
    Usage:
    GET /api/query?topic=myeventhub&partition=0&offset=123456789
    GET /api/query?topic=myeventhub&partition=1&sequence=12345
    GET /api/query?topic=myeventhub&partition=2&latest=true
    """
    
    try:
        logger.info(f"EventHub query request received: {req.url}")
        
        # Get connection string
        connection_string = os.environ.get("EventHubConnectionString")
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"error": "EventHubConnectionString not configured"}),
                status_code=500,
                mimetype="application/json"
            )
        
        # Parse query parameters
        topic = req.params.get('topic') or req.params.get('eventhub')
        partition_id = req.params.get('partition')
        offset = req.params.get('offset')
        sequence = req.params.get('sequence')
        latest = req.params.get('latest', '').lower() == 'true'
        content = req.params.get('content', 'true').lower() == 'true'
        
        # Validate required parameters
        if not topic:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: topic (EventHub name)"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not partition_id:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: partition"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Create EventHub reader
        reader = LatestMessageReader(
            connection_string=connection_string,
            eventhub_name=topic,
            consumer_group="$Default"
        )
        
        start_time = time.time()
        result = {}
        
        try:
            reader.connect()
            
            if latest:
                # Get latest message metadata
                logger.info(f"Getting latest message for {topic}/partition-{partition_id}")
                props = reader.get_partition_properties(partition_id)
                if props and not props.get('isEmpty', True):
                    if content:
                        # Try to get actual message content
                        success = reader.get_latest_message_by_sequence(partition_id, props)
                        result = {
                            "success": success,
                            "topic": topic,
                            "partition": partition_id,
                            "method": "latest_with_content",
                            "properties": props
                        }
                    else:
                        # Just return metadata
                        result = {
                            "success": True,
                            "topic": topic,
                            "partition": partition_id,
                            "method": "latest_metadata_only",
                            "properties": props
                        }
                else:
                    result = {
                        "success": True,
                        "topic": topic,
                        "partition": partition_id,
                        "method": "latest",
                        "message": "Partition is empty",
                        "properties": props
                    }
                    
            elif offset:
                # Query by offset
                logger.info(f"Querying {topic}/partition-{partition_id} at offset {offset}")
                message = reader.get_message_by_offset(partition_id, offset, content)
                result = {
                    "success": message is not None,
                    "topic": topic,
                    "partition": partition_id,
                    "method": "offset_query",
                    "offset": offset,
                    "message": message
                }
                
            elif sequence:
                # Query by sequence
                logger.info(f"Querying {topic}/partition-{partition_id} at sequence {sequence}")
                message = reader.get_message_by_sequence_number(partition_id, sequence, content)
                result = {
                    "success": message is not None,
                    "topic": topic,
                    "partition": partition_id,
                    "method": "sequence_query",
                    "sequence": sequence,
                    "message": message
                }
                
            else:
                return func.HttpResponse(
                    json.dumps({"error": "Must specify one of: offset, sequence, or latest=true"}),
                    status_code=400,
                    mimetype="application/json"
                )
        
        finally:
            reader.disconnect()
        
        # Add timing info
        processing_time = round((time.time() - start_time) * 1000, 2)
        result["processingTimeMs"] = processing_time
        
        logger.info(f"Query completed in {processing_time}ms")
        
        return func.HttpResponse(
            json.dumps(result, default=str, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        error_msg = f"Error processing EventHub query: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )
