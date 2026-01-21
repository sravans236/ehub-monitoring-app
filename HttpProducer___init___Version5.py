import logging
import azure.functions as func
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.eventhub_producer import EventHubProducer, generate_sample_payload


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to send events to Event Hub
    
    Usage:
        POST /api/HttpProducer
        Body: {
            "payload": {... },           // Optional: your data
            "partitionKey": "key",      // Optional: partition key
            "properties": {... },         // Optional: custom properties
            "count": 1,                 // Optional: number of events (default: 1)
            "eventType": "order"        // Optional: sample data type (order/sensor/log)
        }
    
    Examples:
        1. Send single custom event:
           {"payload": {"customerId": "123", "action": "purchase"}}
        
        2. Send 10 sample orders:
           {"count": 10, "eventType":  "order"}
        
        3. Send to specific partition:
           {"payload":  {... }, "partitionKey": "user-123"}
    """
    logging.info('HTTP trigger function for Event Hub producer')
    
    # Get configuration
    connection_string = os.environ.get("EventHubConnectionString")
    eventhub_name = os. environ.get("EventHubName")
    
    if not connection_string or not eventhub_name: 
        return func.HttpResponse(
            json.dumps({
                "error":  "EventHubConnectionString and EventHubName must be configured"
            }),
            status_code=500,
            mimetype="application/json"
        )
    
    try:
        # Parse request body
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        
        # Extract parameters
        payload = req_body.get('payload')
        partition_key = req_body.get('partitionKey')
        properties = req_body. get('properties')
        count = req_body.get('count', 1)
        event_type = req_body.get('eventType', 'default')
        
        # Validate count
        if not isinstance(count, int) or count < 1 or count > 1000:
            return func.HttpResponse(
                json.dumps({"error": "Count must be between 1 and 1000"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Initialize producer
        producer = EventHubProducer(connection_string, eventhub_name)
        
        try:
            if count == 1:
                # Send single event
                if not payload:
                    payload = generate_sample_payload(event_type)
                
                success = producer.send_single_event(
                    payload=payload,
                    partition_key=partition_key,
                    properties=properties
                )
                
                if success:
                    response = {
                        "success": True,
                        "message": "Event sent successfully",
                        "eventhubName": eventhub_name,
                        "eventCount": 1,
                        "partitionKey": partition_key,
                        "payload": payload
                    }
                    return func.HttpResponse(
                        json.dumps(response, indent=2),
                        status_code=200,
                        mimetype="application/json"
                    )
                else: 
                    return func.HttpResponse(
                        json.dumps({"error": "Failed to send event"}),
                        status_code=500,
                        mimetype="application/json"
                    )
            
            else:
                # Send batch of events
                payloads = []
                
                if payload:
                    # Use provided payload as template
                    for i in range(count):
                        event_payload = payload.copy()
                        event_payload["batchIndex"] = i
                        payloads.append(event_payload)
                else:
                    # Generate sample payloads
                    for i in range(count):
                        payloads.append(generate_sample_payload(event_type))
                
                result = producer.send_batch_events(
                    payloads=payloads,
                    partition_key=partition_key,
                    properties=properties
                )
                
                return func.HttpResponse(
                    json.dumps({
                        "success": result["success"],
                        "message": f"Sent {result['sentCount']} events",
                        "eventhubName": eventhub_name,
                        "details": result
                    }, indent=2),
                    status_code=200 if result["success"] else 500,
                    mimetype="application/json"
                )
        
        finally:
            producer.disconnect()
    
    except Exception as e: 
        logging.error(f"Error in HTTP producer: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )