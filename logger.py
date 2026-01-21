import logging
import json
from datetime import datetime
from typing import Dict, Any, List


class LatestMessageLogger: 
    """Logger for Event Hub monitoring activities"""
    
    def __init__(self, logger:  logging.Logger):
        self.logger = logger
    
    def log_latest_message(self, message_data: Dict[str, Any]):
        """Log the latest message with all metadata"""
        log_entry = {
            "eventType": "LatestMessage",
            "timestamp": datetime.utcnow().isoformat(),
            "data": message_data
        }
        
        self.logger.info(f"Latest Message:  {json.dumps(log_entry, default=str)}")
        return log_entry
    
    def log_partition_summary(self, partition_results: List[Dict[str, Any]]):
        """Log summary of all partitions"""
        summary = {
            "eventType": "PartitionSummary",
            "timestamp": datetime.utcnow().isoformat(),
            "totalPartitions": len(partition_results),
            "partitionsWithMessages": sum(1 for r in partition_results if r.get("message") is not None),
            "emptyPartitions": sum(1 for r in partition_results if r.get("status") == "empty"),
            "partitions": partition_results
        }
        
        self.logger.info(f"Partition Summary: {json.dumps(summary, default=str)}")
        return summary
    
    def log_eventhub_summary(self, eventhub_name: str, results: List[Dict[str, Any]]):
        """Log summary for an Event Hub"""
        summary = {
            "eventType": "EventHubSummary",
            "timestamp": datetime.utcnow().isoformat(),
            "eventhubName": eventhub_name,
            "totalPartitions": len(results),
            "messagesFound": sum(1 for r in results if r.get("message") is not None),
            "results": results
        }
        
        self.logger.info(f"Event Hub Summary: {json. dumps(summary, default=str)}")
        return summary
    
    def log_monitor_run_summary(self, all_eventhubs_results: Dict[str, List[Dict[str, Any]]], execution_time_ms: float):
        """Log summary of entire monitoring run"""
        total_eventhubs = len(all_eventhubs_results)
        total_partitions = sum(len(results) for results in all_eventhubs_results.values())
        total_messages = sum(
            sum(1 for r in results if r.get("message") is not None)
            for results in all_eventhubs_results.values()
        )
        
        summary = {
            "eventType": "MonitorRunSummary",
            "timestamp": datetime.utcnow().isoformat(),
            "totalEventHubs": total_eventhubs,
            "totalPartitions":  total_partitions,
            "totalMessagesFound": total_messages,
            "executionTimeMs": execution_time_ms,
            "eventhubs": {
                name: {
                    "partitionCount": len(results),
                    "messagesFound": sum(1 for r in results if r.get("message") is not None)
                }
                for name, results in all_eventhubs_results.items()
            }
        }
        
        self.logger.info(f"Monitor Run Summary: {json.dumps(summary, default=str)}")
        return summary
    
    def log_discovery(self, discovery_method: str, eventhub_count: int, eventhub_names: List[str]):
        """Log Event Hub discovery information"""
        discovery_log = {
            "eventType":  "EventHubDiscovery",
            "timestamp": datetime.utcnow().isoformat(),
            "discoveryMethod": discovery_method,
            "eventhubCount": eventhub_count,
            "eventhubNames": eventhub_names
        }
        
        self. logger.info(f"Event Hub Discovery: {json.dumps(discovery_log, default=str)}")
        return discovery_log
    
    def log_error(self, error_type: str, error_message: str, context: Dict[str, Any] = None):
        """Log error with context"""
        error_log = {
            "eventType": "Error",
            "timestamp": datetime. utcnow().isoformat(),
            "errorType": error_type,
            "errorMessage": error_message,
            "context": context or {}
        }
        
        self.logger. error(f"Error: {json.dumps(error_log, default=str)}")
        return error_log
