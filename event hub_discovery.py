import logging
from typing import List, Dict, Any
from azure.eventhub import EventHubConsumerClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure. identity import DefaultAzureCredential
import re

logger = logging.getLogger("EventHubDiscovery")


class EventHubNamespaceDiscovery:
    """Discovers all Event Hubs in a namespace using connection string"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.namespace = None
        self.namespace_fqdn = None
        self._parse_connection_string()
    
    def _parse_connection_string(self):
        """Parse namespace from connection string"""
        try:
            # Connection string format:  Endpoint=sb://<namespace>.servicebus.windows. net/;SharedAccessKeyName=... ;SharedAccessKey=...
            endpoint_match = re.search(r'Endpoint=sb://([^\.]+)\.servicebus\.windows\. net', self.connection_string)
            if endpoint_match:
                self.namespace = endpoint_match.group(1)
                self.namespace_fqdn = f"{self.namespace}.servicebus. windows.net"
                logger. info(f"Parsed namespace: {self.namespace}")
            else:
                logger.error("Could not parse namespace from connection string")
        except Exception as e:
            logger.error(f"Error parsing connection string:  {str(e)}")
    
    def discover_eventhubs_via_probe(self, known_eventhub_names: List[str] = None) -> List[str]:
        """
        Discover Event Hubs by probing (if you have a list of potential names)
        
        Args: 
            known_eventhub_names: List of potential Event Hub names to test
        
        Returns:
            List of valid Event Hub names
        """
        if not known_eventhub_names: 
            logger.warning("No Event Hub names provided for probing")
            return []
        
        valid_eventhubs = []
        
        for eh_name in known_eventhub_names:
            try: 
                # Try to connect to the Event Hub
                client = EventHubConsumerClient. from_connection_string(
                    conn_str=self.connection_string,
                    consumer_group="$Default",
                    eventhub_name=eh_name
                )
                
                # Try to get partition IDs (this will fail if Event Hub doesn't exist)
                partition_ids = client.get_partition_ids()
                
                logger.info(f"✓ Found Event Hub: {eh_name} with {len(partition_ids)} partitions")
                valid_eventhubs.append(eh_name)
                
                client.close()
            except Exception as e:
                logger.debug(f"✗ Event Hub '{eh_name}' not accessible: {str(e)}")
        
        return valid_eventhubs
    
    def get_namespace_info(self) -> Dict[str, Any]:
        """Get namespace information"""
        return {
            "namespace": self. namespace,
            "namespaceFqdn": self.namespace_fqdn
        }


class EventHubManagementDiscovery:
    """
    Discovers all Event Hubs using Azure Management API
    Requires subscription ID, resource group, and authentication
    """
    
    def __init__(self, subscription_id: str = None, resource_group: str = None, namespace: str = None):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.namespace = namespace
        self.credential = None
        self.mgmt_client = None
        
        if subscription_id and resource_group and namespace:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize management client with Managed Identity"""
        try:
            # Use Managed Identity (works in Azure)
            self.credential = DefaultAzureCredential()
            
            self.mgmt_client = EventHubManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            logger.info("Management client initialized successfully")
        except Exception as e:
            logger.debug(f"Could not initialize management client: {str(e)}")
    
    def discover_all_eventhubs(self) -> List[Dict[str, Any]]:
        """
        Discover all Event Hubs in the namespace using Management API
        
        Returns: 
            List of Event Hub information dictionaries
        """
        eventhubs = []
        
        if not self.mgmt_client:
            logger.warning("Management client not available.  Cannot discover Event Hubs.")
            return eventhubs
        
        try:
            eventhubs_list = self.mgmt_client. event_hubs.list_by_namespace(
                resource_group_name=self.resource_group,
                namespace_name=self.namespace
            )
            
            for eh in eventhubs_list: 
                eventhub_info = {
                    "name": eh.name,
                    "partitionCount": eh.partition_count,
                    "status": eh.status,
                    "partitionIds": eh.partition_ids or [],
                    "createdAt": eh.created_at. isoformat() if eh.created_at else None,
                    "updatedAt": eh.updated_at.isoformat() if eh.updated_at else None
                }
                eventhubs.append(eventhub_info)
                logger.info(f"Discovered Event Hub: {eh.name} with {eh.partition_count} partitions")
            
            logger.info(f"Total Event Hubs discovered: {len(eventhubs)}")
        
        except Exception as e: 
            logger.debug(f"Error discovering Event Hubs via Management API: {str(e)}")
        
        return eventhubs
    
    def get_eventhub_names(self) -> List[str]:
        """Get list of Event Hub names only"""
        eventhubs = self.discover_all_eventhubs()
        return [eh["name"] for eh in eventhubs]


def discover_eventhubs(
    connection_string: str,
    subscription_id: str = None,
    resource_group: str = None,
    fallback_names: List[str] = None
) -> List[str]:
    """
    Unified discovery function that tries multiple methods
    
    Priority: 
    1. Try Management API (if credentials available)
    2. Fall back to provided list
    3. Return empty list if nothing works
    
    Args:
        connection_string: Event Hub connection string
        subscription_id: Azure subscription ID (optional)
        resource_group:  Resource group name (optional)
        fallback_names:  Fallback list of Event Hub names (optional)
    
    Returns:
        List of Event Hub names
    """
    logger.info("Starting Event Hub discovery...")
    
    # Parse namespace from connection string
    cs_discovery = EventHubNamespaceDiscovery(connection_string)
    namespace_info = cs_discovery.get_namespace_info()
    namespace = namespace_info["namespace"]
    
    logger.info(f"Namespace: {namespace}")
    
    # Method 1: Try Management API (requires Managed Identity or credentials)
    if subscription_id and resource_group and namespace:
        logger.info("Attempting discovery via Management API...")
        try:
            mgmt_discovery = EventHubManagementDiscovery(
                subscription_id=subscription_id,
                resource_group=resource_group,
                namespace=namespace
            )
            
            eventhub_names = mgmt_discovery. get_eventhub_names()
            
            if eventhub_names: 
                logger.info(f"✓ Discovered {len(eventhub_names)} Event Hubs via Management API")
                return eventhub_names
            else: 
                logger.warning("Management API returned no Event Hubs")
        except Exception as e:
            logger.debug(f"Management API discovery failed: {str(e)}")
    else:
        logger.info("Management API credentials not provided, skipping...")
    
    # Method 2: Use fallback list if provided
    if fallback_names: 
        logger.info(f"Using fallback list with {len(fallback_names)} Event Hub names")
        # Optionally probe to verify they exist
        valid_names = cs_discovery.discover_eventhubs_via_probe(fallback_names)
        if valid_names:
            logger.info(f"✓ Verified {len(valid_names)} Event Hubs from fallback list")
            return valid_names
        else:
            logger.warning("Could not verify Event Hubs from fallback list, using as-is")
            return fallback_names
    
    # Method 3: Nothing worked
    logger.error("Could not discover Event Hubs.  Please provide either:")
    logger.error("  1.  Subscription ID + Resource Group (for Management API)")
    logger.error("  2. Fallback list of Event Hub names")
    
    return []
