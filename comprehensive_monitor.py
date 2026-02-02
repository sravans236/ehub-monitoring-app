"""
Comprehensive EventHub Monitoring Solution
Provides detailed monitoring of EventHub namespaces, hubs, partitions, and consumer groups
with rich data classes and structured logging.
"""

import logging
from datetime import datetime, timezone
import os
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field, asdict
from azure.eventhub import EventHubConsumerClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.identity import DefaultAzureCredential
import json
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import ClientSecretCredential

logger = logging.getLogger("ComprehensiveEventHubMonitor")


@dataclass
class PartitionMetrics:
    """Metrics for a single EventHub partition"""
    namespace: str
    hub_name: str
    partition_id: str
    beginning_sequence_number: Optional[int] = None
    last_enqueued_sequence_number: Optional[int] = None
    last_enqueued_offset: Optional[str] = None
    last_enqueued_time_utc: Optional[datetime] = None
    is_empty: bool = True
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def total_available_messages(self) -> Optional[int]:
        """Calculate total available messages in partition"""
        if (self.beginning_sequence_number is not None and 
            self.last_enqueued_sequence_number is not None and 
            not self.is_empty):
            return self.last_enqueued_sequence_number - self.beginning_sequence_number + 1
        return 0 if self.is_empty else None

    @property
    def retention_age_hours(self) -> Optional[float]:
        """Calculate approximate age of oldest retained message in hours"""
        if self.last_enqueued_time_utc:
            age_delta = datetime.now(timezone.utc) - self.last_enqueued_time_utc
            return age_delta.total_seconds() / 3600
        return None

    @property
    def has_recent_activity(self) -> bool:
        """Check if partition has recent activity (within last hour)"""
        if self.last_enqueued_time_utc:
            time_since_last = datetime.now(timezone.utc) - self.last_enqueued_time_utc
            return time_since_last.total_seconds() < 3600  # 1 hour
        return False


@dataclass
class ConsumerGroupLag:
    """Lag metrics for a consumer group on a specific partition"""
    namespace: str
    hub_name: str
    consumer_group: str
    partition_id: str
    latest_sequence_number: Optional[int] = None
    current_sequence_number: Optional[int] = None
    sequence_lag: Optional[int] = None
    latest_offset: Optional[str] = None
    current_offset: Optional[str] = None
    time_lag_seconds: Optional[float] = None
    latest_enqueued_time: Optional[datetime] = None
    is_empty: bool = True
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_healthy(self) -> bool:
        """Check if consumer group is healthy (partition is active and not erroring)"""
        return not self.error and not self.is_empty

    @property
    def lag_status(self) -> str:
        """Get partition status as string"""
        if self.error:
            return "ERROR"
        if self.is_empty:
            return "EMPTY"
        return "ACTIVE"


@dataclass
class ConsumerGroupMetrics:
    """Comprehensive metrics for a consumer group across all partitions"""
    namespace: str
    hub_name: str
    consumer_group: str
    partition_lags: List[ConsumerGroupLag] = field(default_factory=list)
    partition_metrics: List[PartitionMetrics] = field(default_factory=list)
    total_partitions: int = 0
    active_partitions: int = 0
    empty_partitions: int = 0
    partitions_with_errors: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def calculate_aggregates(self):
        """Calculate aggregate metrics from partition status"""
        self.total_partitions = len(self.partition_lags)
        self.active_partitions = sum(1 for lag in self.partition_lags if lag.is_healthy)
        self.empty_partitions = sum(1 for lag in self.partition_lags if lag.is_empty and not lag.error)
        self.partitions_with_errors = sum(1 for lag in self.partition_lags if lag.error)

    @property
    def activity_score(self) -> float:
        """Calculate activity score (0-100) - percentage of partitions that are active (not empty, no errors)"""
        if self.total_partitions == 0:
            return 0.0
        return (self.active_partitions / self.total_partitions) * 100

    @property
    def total_available_messages(self) -> int:
        """Calculate total available messages across all partitions for this consumer group"""
        total = 0
        for partition_metric in self.partition_metrics:
            if partition_metric.total_available_messages is not None:
                total += partition_metric.total_available_messages
        return total

    @property
    def partition_message_details(self) -> List[Dict[str, Any]]:
        """Get partition-level message details for table display"""
        details = []
        for partition_metric in self.partition_metrics:
            details.append({
                "consumer_group": self.consumer_group,
                "partition_id": partition_metric.partition_id,
                "total_available_messages": partition_metric.total_available_messages or 0,
                "is_empty": partition_metric.is_empty,
                "last_enqueued_time": partition_metric.last_enqueued_time_utc,
                "beginning_sequence_number": partition_metric.beginning_sequence_number,
                "last_enqueued_sequence_number": partition_metric.last_enqueued_sequence_number
            })
        return details


@dataclass
class EventHubMetrics:
    """Comprehensive metrics for an EventHub"""
    namespace: str
    hub_name: str
    partition_count: int
    partition_metrics: List[PartitionMetrics] = field(default_factory=list)
    consumer_groups: List[ConsumerGroupMetrics] = field(default_factory=list)
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def total_consumer_groups(self) -> int:
        return len(self.consumer_groups)

    @property
    def active_consumer_groups(self) -> int:
        """Consumer groups with high activity (>= 90% active partitions)"""
        return sum(1 for cg in self.consumer_groups if cg.activity_score >= 90)


@dataclass
class NamespaceMetrics:
    """Comprehensive metrics for an EventHub namespace"""
    namespace: str
    discovery_time: datetime
    eventhubs: List[EventHubMetrics] = field(default_factory=list)
    total_eventhubs: int = 0
    total_partitions: int = 0
    total_consumer_groups: int = 0
    discovery_method: str = "unknown"
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def calculate_aggregates(self):
        """Calculate aggregate metrics"""
        self.total_eventhubs = len(self.eventhubs)
        self.total_partitions = sum(eh.partition_count for eh in self.eventhubs)
        self.total_consumer_groups = sum(eh.total_consumer_groups for eh in self.eventhubs)


class ComprehensiveEventHubMonitor:
    """
    Comprehensive EventHub monitoring solution that discovers and monitors:
    - All EventHubs in namespace
    - All partitions for each EventHub (queried once per partition)
    - All consumer groups for each EventHub
    - Detailed activity metrics for each consumer group/partition combination
    
    Optimized to avoid redundant EventHub connections by reusing partition data.
    """

    def __init__(self, 
                 connection_string: str,
                 subscription_id: Optional[str] = None,
                 resource_group: Optional[str] = None,
                 max_workers: int = 10):
        """
        Initialize comprehensive monitor
        
        Args:
            connection_string: EventHub connection string
            subscription_id: Azure subscription ID (for Management API)
            resource_group: Resource group name (for Management API)  
            max_workers: Maximum concurrent workers for parallel processing
        """
        self.connection_string = connection_string
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.max_workers = max_workers
        
        # Parse namespace from connection string
        self.namespace = self._parse_namespace()
        
        # Initialize management client if credentials available
        self.mgmt_client = None
        if subscription_id and resource_group:
            try:    
                client_id = os.environ.get("AZURE_CLIENT_ID")
                client_secret = os.environ.get("AZURE_CLIENT_SECRET")
                tenant_id = os.environ.get("AZURE_TENANT_ID")
                
                credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
                # credential = DefaultAzureCredential()
                self.mgmt_client = EventHubManagementClient(
                    credential=credential,
                    subscription_id=subscription_id
                )
                logger.info("Management client initialized successfully using ClientSecretCredential")
            except Exception as e:
                logger.warning(f"Could not initialize management client: {e}")

        # Cache for clients to avoid recreating them
        self._client_cache = {}

    def _parse_namespace(self) -> Optional[str]:
        """Parse namespace from connection string"""
        try:
            match = re.search(r'Endpoint=sb://([^\.]+)\.servicebus\.windows\.net', self.connection_string)
            if match:
                namespace = match.group(1)
                logger.info(f"Parsed namespace: {namespace}")
                return namespace
            else:
                logger.error("Could not parse namespace from connection string")
                return None
        except Exception as e:
            logger.error(f"Error parsing connection string: {e}")
            return None

    def get_client(self, eventhub_name: str, consumer_group: str = "$Default") -> EventHubConsumerClient:
        """Get cached client for EventHub/consumer group combination"""
        key = f"{eventhub_name}::{consumer_group}"
        if key not in self._client_cache:
            self._client_cache = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=consumer_group,
                eventhub_name=eventhub_name
            )
        return self._client_cache

    def close_all_clients(self):
        """Close all cached clients"""
        for client in self._client_cache.values():
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")
        self._client_cache.clear()

    def discover_eventhubs(self) -> List[str]:
        """Discover all EventHubs in namespace"""
        eventhub_names = []
        
        if self.mgmt_client:
            logger.info("Discovering EventHubs via Management API...")
            try:
                eventhubs_list = self.mgmt_client.event_hubs.list_by_namespace(
                    resource_group_name=self.resource_group,
                    namespace_name=self.namespace
                )
                
                for eh in eventhubs_list:
                    eventhub_names.append(eh.name)
                    logger.debug(f"Found EventHub: {eh.name}")
                
                logger.info(f"Discovered {len(eventhub_names)} EventHubs via Management API")
                return eventhub_names
                
            except Exception as e:
                logger.warning(f"Management API discovery failed: {e}")
        
        logger.warning("Could not discover EventHubs. Management API not available.")
        return []

    def discover_consumer_groups(self, eventhub_name: str) -> List[str]:
        """Discover all consumer groups for an EventHub"""
        consumer_groups = []
        
        if self.mgmt_client:
            try:
                cg_list = self.mgmt_client.consumer_groups.list_by_event_hub(
                    resource_group_name=self.resource_group,
                    namespace_name=self.namespace,
                    event_hub_name=eventhub_name
                )
                
                for cg in cg_list:
                    consumer_groups.append(cg.name)
                    logger.debug(f"Found consumer group: {cg.name} for EventHub: {eventhub_name}")
                
                logger.info(f"Discovered {len(consumer_groups)} consumer groups for {eventhub_name}")
                
            except Exception as e:
                logger.warning(f"Error discovering consumer groups for {eventhub_name}: {e}")
                # Fallback to default consumer group
                consumer_groups = ["$Default"]
        else:
            # Without management API, assume default consumer group exists
            consumer_groups = ["$Default"]
            logger.info(f"Using default consumer group for {eventhub_name} (Management API not available)")
        
        return consumer_groups

    def get_partition_metrics(self, eventhub_name: str) -> List[PartitionMetrics]:
        """Get metrics for all partitions of an EventHub"""
        partition_metrics = []
        
        try:
            client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group="$Default",
                eventhub_name=eventhub_name
            )
            partition_ids = client.get_partition_ids()
            
            logger.info(f"Getting metrics for {len(partition_ids)} partitions in {eventhub_name} (will be reused for all consumer groups)")
            
            for partition_id in partition_ids:
                try:
                    props = client.get_partition_properties(partition_id)
                    logger.info(f"*Retrieved properties for partition {partition_id} in {eventhub_name}, props: {props}")
                    metrics = PartitionMetrics(
                        namespace=self.namespace,
                        hub_name=eventhub_name,
                        partition_id=partition_id,
                        beginning_sequence_number=getattr(props, 'beginning_sequence_number', None),
                        last_enqueued_sequence_number=getattr(props, 'last_enqueued_sequence_number', None),
                        last_enqueued_offset=getattr(props, 'last_enqueued_offset', None),
                        last_enqueued_time_utc=getattr(props, 'last_enqueued_time_utc', None),
                        is_empty=getattr(props, 'is_empty', True)
                    )
                    
                    partition_metrics.append(metrics)
                    
                except Exception as e:
                    logger.error(f"Error getting metrics for partition {partition_id}: {e}")
                    # Create error partition metrics
                    error_metrics = PartitionMetrics(
                        namespace=self.namespace,
                        hub_name=eventhub_name,
                        partition_id=partition_id,
                        is_empty=True
                    )
                    partition_metrics.append(error_metrics)
            
        except Exception as e:
            logger.error(f"Error getting partition metrics for {eventhub_name}: {e}")
        
        return partition_metrics

    def get_consumer_group_status(self, eventhub_name: str, consumer_group: str, partition_metrics: List[PartitionMetrics]) -> ConsumerGroupMetrics:
        """Get comprehensive status metrics for a consumer group using existing partition data"""
        logger.info(f"Getting status for consumer group '{consumer_group}' on EventHub '{eventhub_name}'")
        
        partition_lags = []
        
        # Create ConsumerGroupLag objects using existing partition data
        for partition_metric in partition_metrics:
            consumer_lag = ConsumerGroupLag(
                namespace=self.namespace,
                hub_name=eventhub_name,
                consumer_group=consumer_group,
                partition_id=partition_metric.partition_id,
                latest_sequence_number=partition_metric.last_enqueued_sequence_number,
                current_sequence_number=None,
                sequence_lag=None,
                latest_offset=partition_metric.last_enqueued_offset,
                current_offset=None,
                time_lag_seconds=None,
                latest_enqueued_time=partition_metric.last_enqueued_time_utc,
                is_empty=partition_metric.is_empty,
                error=None
            )
            partition_lags.append(consumer_lag)
        
        # Create comprehensive consumer group metrics
        cg_metrics = ConsumerGroupMetrics(
            namespace=self.namespace,
            hub_name=eventhub_name,
            consumer_group=consumer_group,
            partition_lags=partition_lags,
            partition_metrics=partition_metrics
        )
        
        # Calculate aggregates
        cg_metrics.calculate_aggregates()
        
        logger.info(f"Consumer group '{consumer_group}' status: "
                   f"Activity={cg_metrics.activity_score:.1f}%, "
                   f"Active={cg_metrics.active_partitions}/{cg_metrics.total_partitions}, "
                   f"TotalMessages={cg_metrics.total_available_messages}")
        
        return cg_metrics


    def monitor_eventhub(self, eventhub_name: str) -> EventHubMetrics:
        """Get comprehensive metrics for a single EventHub"""
        logger.info(f"Monitoring EventHub: {eventhub_name}")
        start_time = time.time()
        
        try:
            # Get basic EventHub info
            eventhub_info = None
            if self.mgmt_client:
                try:
                    eh = self.mgmt_client.event_hubs.get(
                        resource_group_name=self.resource_group,
                        namespace_name=self.namespace,
                        event_hub_name=eventhub_name
                    )
                    eventhub_info = {
                        'partition_count': eh.partition_count,
                        'status': eh.status,
                        'created_at': eh.created_at,
                        'updated_at': eh.updated_at
                    }
                except Exception as e:
                    logger.warning(f"Could not get EventHub info via Management API: {e}")
            
            # Get partition count from direct connection if Management API failed
            if not eventhub_info:
                try:
                    client = self.get_client(eventhub_name)
                    partition_ids = client.get_partition_ids()
                    eventhub_info = {
                        'partition_count': len(partition_ids),
                        'status': 'Active',
                        'created_at': None,
                        'updated_at': None
                    }
                except Exception as e:
                    logger.error(f"Could not determine partition count for {eventhub_name}: {e}")
                    eventhub_info = {
                        'partition_count': 0,
                        'status': 'Unknown',
                        'created_at': None,
                        'updated_at': None
                    }
            
            # Get partition metrics 
            partition_metrics = self.get_partition_metrics(eventhub_name)
            
            # Discover consumer groups
            consumer_group_names = self.discover_consumer_groups(eventhub_name)
            
            # Monitor each consumer group using existing partition data
            consumer_group_metrics = []
            if consumer_group_names:
                for cg_name in consumer_group_names:
                    try:
                        cg_metrics = self.get_consumer_group_status(eventhub_name, cg_name, partition_metrics)
                        consumer_group_metrics.append(cg_metrics)
                    except Exception as e:
                        logger.error(f"Error monitoring consumer group {cg_name}: {e}")
                        # Create error metrics
                        error_lag = ConsumerGroupLag(
                            namespace=self.namespace,
                            hub_name=eventhub_name,
                            consumer_group=cg_name,
                            partition_id="all",
                            error=str(e)
                        )
                        error_metrics = ConsumerGroupMetrics(
                            namespace=self.namespace,
                            hub_name=eventhub_name,
                            consumer_group=cg_name,
                            partition_lags=[error_lag],
                            partition_metrics=partition_metrics
                        )
                        consumer_group_metrics.append(error_metrics)
            
            # Create EventHub metrics
            eh_metrics = EventHubMetrics(
                namespace=self.namespace,
                hub_name=eventhub_name,
                partition_count=eventhub_info['partition_count'],
                partition_metrics=partition_metrics,
                consumer_groups=consumer_group_metrics,
                status=eventhub_info['status'],
                created_at=eventhub_info['created_at'],
                updated_at=eventhub_info['updated_at']
            )
            
            processing_time = time.time() - start_time
            logger.info(f"Completed monitoring EventHub '{eventhub_name}' in {processing_time:.2f}s: "
                       f"{len(partition_metrics)} partitions, {len(consumer_group_metrics)} consumer groups")
            
            return eh_metrics
            
        except Exception as e:
            logger.error(f"Error monitoring EventHub {eventhub_name}: {e}")
            # Return minimal error metrics
            return EventHubMetrics(
                namespace=self.namespace,
                hub_name=eventhub_name,
                partition_count=0,
                status="Error"
            )

    def monitor_namespace(self) -> NamespaceMetrics:
        """Monitor entire namespace comprehensively"""
        logger.info(f"Starting comprehensive monitoring of namespace: {self.namespace}")
        start_time = time.time()
        
        discovery_time = datetime.now(timezone.utc)
        
        # Discover all EventHubs
        eventhub_names = self.discover_eventhubs()
        
        if not eventhub_names:
            logger.warning("No EventHubs discovered in namespace")
            return NamespaceMetrics(
                namespace=self.namespace,
                discovery_time=discovery_time,
                discovery_method="failed",
                subscription_id=self.subscription_id,
                resource_group=self.resource_group
            )
        
        logger.info(f"Discovered {len(eventhub_names)} EventHubs, starting detailed monitoring...")
        
        # Monitor all EventHubs (parallel processing)
        eventhub_metrics = []
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(eventhub_names))) as executor:
            future_to_eh = {
                executor.submit(self.monitor_eventhub, eh_name): eh_name 
                for eh_name in eventhub_names
            }
            
            for future in as_completed(future_to_eh):
                eh_name = future_to_eh[future]
                try:
                    eh_metrics = future.result()
                    eventhub_metrics.append(eh_metrics)
                except Exception as e:
                    logger.error(f"Error monitoring EventHub {eh_name}: {e}")
        
        # Create comprehensive namespace metrics
        namespace_metrics = NamespaceMetrics(
            namespace=self.namespace,
            discovery_time=discovery_time,
            eventhubs=eventhub_metrics,
            discovery_method="management_api" if self.mgmt_client else "fallback",
            subscription_id=self.subscription_id,
            resource_group=self.resource_group
        )
        
        # Calculate aggregates
        namespace_metrics.calculate_aggregates()
        
        processing_time = time.time() - start_time
        logger.info(f"Completed comprehensive namespace monitoring in {processing_time:.2f}s: "
                   f"{namespace_metrics.total_eventhubs} EventHubs, "
                   f"{namespace_metrics.total_partitions} partitions, "
                   f"{namespace_metrics.total_consumer_groups} consumer groups")
        
        return namespace_metrics

    def get_partition_message_table(self, namespace_metrics: NamespaceMetrics) -> List[Dict[str, Any]]:
        """Get partition message details across all consumer groups in table format"""
        table_data = []
        for eventhub in namespace_metrics.eventhubs:
            for consumer_group in eventhub.consumer_groups:
                for detail in consumer_group.partition_message_details:
                    table_data.append({
                        "namespace": namespace_metrics.namespace,
                        "eventhub": eventhub.hub_name,
                        "consumer_group": detail["consumer_group"],
                        "partition_id": detail["partition_id"],
                        "total_available_messages": detail["total_available_messages"],
                        "is_empty": detail["is_empty"],
                        "last_enqueued_time": detail["last_enqueued_time"],
                        "beginning_sequence_number": detail["beginning_sequence_number"],
                        "last_enqueued_sequence_number": detail["last_enqueued_sequence_number"]
                    })
        return table_data

    def print_partition_message_table(self, namespace_metrics: NamespaceMetrics):
        """Print partition message details in a formatted table"""
        table_data = self.get_partition_message_table(namespace_metrics)
        
        if not table_data:
            logger.info("No partition data available")
            return
        
        # Print header
        header = f"{'Namespace':<15} {'EventHub':<15} {'Consumer Group':<20} {'Partition':<10} {'Available Messages':<18} {'Status':<10}"
        logger.info("=" * len(header))
        logger.info(header)
        logger.info("=" * len(header))
        
        # Print data rows
        for row in table_data:
            status = "EMPTY" if row["is_empty"] else "ACTIVE"
            logger.info(f"{row['namespace']:<15} {row['eventhub']:<15} {row['consumer_group']:<20} "
                       f"{row['partition_id']:<10} {row['total_available_messages']:<18} {status:<10}")
        
        logger.info("=" * len(header))

    def __del__(self):
        """Cleanup on destruction"""
        self.close_all_clients()


class ComprehensiveMonitorLogger:
    """Enhanced logger for comprehensive monitoring data"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_namespace_metrics(self, metrics: NamespaceMetrics):
        """Log comprehensive namespace metrics"""
        log_data = {
            "eventType": "NamespaceComprehensiveMetrics",
            "timestamp": metrics.timestamp.isoformat(),
            "namespace": metrics.namespace,
            "discoveryTime": metrics.discovery_time.isoformat(),
            "discoveryMethod": metrics.discovery_method,
            "subscriptionId": metrics.subscription_id,
            "resourceGroup": metrics.resource_group,
            "totals": {
                "eventhubs": metrics.total_eventhubs,
                "partitions": metrics.total_partitions,
                "consumerGroups": metrics.total_consumer_groups
            },
            "eventhubs": [
                {
                    "name": eh.hub_name,
                    "partitionCount": eh.partition_count,
                    "consumerGroupCount": eh.total_consumer_groups,
                    "activeConsumerGroups": eh.active_consumer_groups,
                    "status": eh.status
                }
                for eh in metrics.eventhubs
            ]
        }
        self.logger.info(f"Namespace Comprehensive Metrics: {json.dumps(log_data, default=str)}")

    def log_eventhub_metrics(self, metrics: EventHubMetrics):
        """Log comprehensive EventHub metrics"""
        log_data = {
            "eventType": "EventHubComprehensiveMetrics",
            "timestamp": metrics.timestamp.isoformat(),
            "namespace": metrics.namespace,
            "hubName": metrics.hub_name,
            "partitionCount": metrics.partition_count,
            "status": metrics.status,
            "consumerGroupCount": metrics.total_consumer_groups,
            "activeConsumerGroups": metrics.active_consumer_groups,
            "partitionMetrics": [asdict(pm) for pm in metrics.partition_metrics],
            "consumerGroupSummary": [
                {
                    "name": cg.consumer_group,
                    "activityScore": cg.activity_score,
                    "activePartitions": cg.active_partitions,
                    "totalPartitions": cg.total_partitions,
                    "emptyPartitions": cg.empty_partitions,
                    "partitionsWithErrors": cg.partitions_with_errors,
                    "totalAvailableMessages": cg.total_available_messages
                }
                for cg in metrics.consumer_groups
            ]
        }
        self.logger.info(f"EventHub Comprehensive Metrics: {json.dumps(log_data, default=str, indent=2)}")

    def log_consumer_group_activity(self, metrics: ConsumerGroupMetrics):
        """Log detailed consumer group activity metrics"""
        log_data = {
            "eventType": "ConsumerGroupActivityDetailed",
            "timestamp": metrics.timestamp.isoformat(),
            "namespace": metrics.namespace,
            "hubName": metrics.hub_name,
            "consumerGroup": metrics.consumer_group,
            "activityScore": metrics.activity_score,
            "totals": {
                "partitions": metrics.total_partitions,
                "activePartitions": metrics.active_partitions,
                "emptyPartitions": metrics.empty_partitions,
                "partitionsWithErrors": metrics.partitions_with_errors,
                "totalAvailableMessages": metrics.total_available_messages
            },
            "partitionDetails": [asdict(status) for status in metrics.partition_lags]
        }
        self.logger.info(f"Consumer Group Activity Detailed: {json.dumps(log_data, default=str)}")

    def log_partition_message_table(self, namespace_metrics: NamespaceMetrics):
        """Log partition message table data in KQL-friendly format"""
        table_data = []
        for eventhub in namespace_metrics.eventhubs:
            for consumer_group in eventhub.consumer_groups:
                for detail in consumer_group.partition_message_details:
                    table_data.append({
                        "namespace": namespace_metrics.namespace,
                        "eventhub": eventhub.hub_name,
                        "consumerGroup": detail["consumer_group"],
                        "partitionId": detail["partition_id"],
                        "totalAvailableMessages": detail["total_available_messages"],
                        "isEmpty": detail["is_empty"],
                        "lastEnqueuedTime": detail["last_enqueued_time"].isoformat() if detail["last_enqueued_time"] else None,
                        "beginningSequenceNumber": detail["beginning_sequence_number"],
                        "lastEnqueuedSequenceNumber": detail["last_enqueued_sequence_number"]
                    })
        
        log_data = {
            "eventType": "PartitionMessageTable",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "namespace": namespace_metrics.namespace,
            "partitionData": table_data,
            "totalPartitions": len(table_data),
            "totalMessages": sum(row["totalAvailableMessages"] for row in table_data),
            "activePartitions": sum(1 for row in table_data if not row["isEmpty"]),
            "emptyPartitions": sum(1 for row in table_data if row["isEmpty"])
        }
        self.logger.info(f"Partition Message Table: {json.dumps(log_data, default=str)}")

    def log_monitoring_summary(self, processing_time: float, namespace_metrics: NamespaceMetrics):
        """Log summary of monitoring run"""
        summary = {
            "eventType": "MonitoringSummary",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "namespace": namespace_metrics.namespace,
            "processingTimeSeconds": processing_time,
            "discovered": {
                "eventhubs": namespace_metrics.total_eventhubs,
                "partitions": namespace_metrics.total_partitions,
                "consumerGroups": namespace_metrics.total_consumer_groups
            },
            "health": {
                "activeEventHubs": sum(1 for eh in namespace_metrics.eventhubs if eh.active_consumer_groups > 0),
                "totalConsumerGroups": namespace_metrics.total_consumer_groups,
                "activeConsumerGroups": sum(eh.active_consumer_groups for eh in namespace_metrics.eventhubs)
            }
        }
        self.logger.info(f"Monitoring Summary: {json.dumps(summary, default=str)}")
