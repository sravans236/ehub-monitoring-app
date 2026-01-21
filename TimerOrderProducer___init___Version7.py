import logging
import azure.functions as func
from datetime import datetime
import sys
import os

sys.path. append(os.path.dirname(os.path.dirname(__file__)))

from utils.eventhub_producer import EventHubProducer, generate_sample_payload


def main(timer: func.TimerRequest) -> None:
    """
    Timer-triggered function to send order events to Event Hub
    Runs every 2 minutes and sends 5 sample order events
    """
    utc_timestamp = datetime.utcnow().isoformat()
    
    if timer.past_due:
        logging.info('The timer is past due!')
    
    logging.info(f'Timer Order Producer triggered at {utc_timestamp}')
    
    # Get configuration
    connection_string = os. environ. get("EventHubConnectionString")
    eventhub_name = os.environ.get("EventHubName")
    
    if not connection_string or not eventhub_name: 
        logging.error("EventHubConnectionString and EventHubName must be configured")
        return
    
    # Configuration for this run
    events_to_send = 5
    event_type = "order"
    
    try:
        # Initialize producer
        producer = EventHubProducer(connection_string, eventhub_name)
        
        logging.info(f"Generating {events_to_send} {event_type} events...")
        
        # Generate order payloads
        payloads = []
        for i in range(events_to_send):
            payload = generate_sample_payload(event_type)
            payload["batchIndex"] = i
            payload["timerRun"] = utc_timestamp
            payload["runId"] = f"RUN-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            payloads.append(payload)
            
            logging.info(f"  Order {i+1}:  OrderID={payload['orderId']}, Amount=${payload['amount']}")
        
        # Send batch to Event Hub
        logging.info(f"Sending {events_to_send} orders to Event Hub:  {eventhub_name}")
        result = producer.send_batch_events(payloads=payloads)
        
        if result['success']:
            logging.info(
                f"✓ Successfully sent {result['sentCount']} orders "
                f"(Failed: {result['failedCount']})"
            )
        else:
            logging.error(
                f"✗ Failed to send orders.  "
                f"Sent:  {result['sentCount']}, Failed: {result['failedCount']}"
            )
        
        producer.disconnect()
        
        # Log summary
        logging.info(f"Timer Order Producer completed at {datetime.utcnow().isoformat()}")
    
    except Exception as e: 
        logging.error(f"Error in timer order producer: {str(e)}", exc_info=True)