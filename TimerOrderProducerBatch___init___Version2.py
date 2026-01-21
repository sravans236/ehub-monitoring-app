import logging
import azure.functions as func
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.eventhub_producer import EventHubProducer, generate_sample_payload


def main(timer: func. TimerRequest) -> None:
    """
    Timer-triggered function to send order events in batch
    More efficient than sending one-by-one
    Runs every 2 minutes and sends 5 sample orders as a batch
    """
    utc_timestamp = datetime.utcnow().isoformat()
    
    if timer.past_due:
        logging.info('The timer is past due!')
    
    logging.info(f'Timer Order Producer (Batch) triggered at {utc_timestamp}')
    
    # Get configuration
    connection_string = os.environ.get("EventHubConnectionString")
    eventhub_name = os.environ.get("EventHubName")
    orders_per_run = int(os.environ.get("OrdersPerRun", "5"))
    
    if not connection_string or not eventhub_name:
        logging.error("EventHubConnectionString and EventHubName must be configured")
        return
    
    try:
        # Initialize producer
        producer = EventHubProducer(connection_string, eventhub_name)
        
        # Generate run ID for this batch
        run_id = f"RUN-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        logging.info(f"Generating {orders_per_run} order events (Run ID: {run_id})...")
        
        # Generate all order payloads
        payloads = []
        for i in range(orders_per_run):
            payload = generate_sample_payload("order")
            payload["batchIndex"] = i + 1
            payload["totalInBatch"] = orders_per_run
            payload["timerRun"] = utc_timestamp
            payload["runId"] = run_id
            
            payloads.append(payload)
            
            logging.info(
                f"  Order {i+1}:  "
                f"OrderID={payload['orderId']}, "
                f"Customer={payload['customerId']}, "
                f"Amount=${payload['amount']}, "
                f"Status={payload['status']}"
            )
        
        # Add custom properties for the entire batch
        batch_properties = {
            "source": "timer-producer-batch",
            "eventType":  "order-batch",
            "runId": run_id,
            "batchSize": orders_per_run
        }
        
        # Send entire batch to Event Hub
        logging.info(f"\nSending batch of {orders_per_run} orders to Event Hub: {eventhub_name}")
        result = producer.send_batch_events(
            payloads=payloads,
            properties=batch_properties
        )
        
        # Log result
        if result['success']: 
            logging.info(
                f"\n{'='*60}\n"
                f"✓ BATCH SENT SUCCESSFULLY\n"
                f"{'='*60}\n"
                f"  Run ID: {run_id}\n"
                f"  Event Hub: {eventhub_name}\n"
                f"  Total Orders: {result['totalEvents']}\n"
                f"  Successfully Sent: {result['sentCount']}\n"
                f"  Failed: {result['failedCount']}\n"
                f"  Completed At: {datetime.utcnow().isoformat()}\n"
                f"{'='*60}"
            )
        else:
            logging.error(
                f"\n{'='*60}\n"
                f"✗ BATCH SEND FAILED\n"
                f"{'='*60}\n"
                f"  Run ID:  {run_id}\n"
                f"  Error: {result. get('error', 'Unknown error')}\n"
                f"  Sent: {result['sentCount']}\n"
                f"  Failed: {result['failedCount']}\n"
                f"{'='*60}"
            )
        
        producer.disconnect()
    
    except Exception as e: 
        logging.error(f"Error in timer order producer (batch): {str(e)}", exc_info=True)