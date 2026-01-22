"""
Centralized logging configuration for EventHub monitoring app.
Suppresses verbose logs from Azure libraries while maintaining app-level logging.
"""

import logging
import os


def configure_logging(app_level: str = "INFO"):
    """
    Configure logging levels to suppress verbose Azure library logs
    
    Args:
        app_level: Logging level for the application loggers (DEBUG, INFO, WARNING, ERROR)
    """
    
    # Convert string level to logging constant
    app_log_level = getattr(logging, app_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=app_log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Suppress verbose Azure library logs
    azure_loggers = [
        'azure.eventhub',
        'azure.core',
        'azure.identity',
        'azure.storage',
        'azure.servicebus',
        'uamqp',
        'azure.eventhub._transport',
        'azure.eventhub._consumer',
        'azure.eventhub._producer',
        'azure.eventhub._client',
        'azure.eventhub.amqp',
        'azure.core.pipeline',
        'azure.core.pipeline.policies',
        'azure.core.pipeline.transport',
        'msal',
    ]
    
    # Set all Azure-related loggers to WARNING level or higher
    for logger_name in azure_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    
    # Specifically suppress AMQP connection logs
    logging.getLogger('uamqp.connection').setLevel(logging.ERROR)
    logging.getLogger('uamqp.receiver').setLevel(logging.ERROR)
    logging.getLogger('uamqp.sender').setLevel(logging.ERROR)
    logging.getLogger('uamqp.session').setLevel(logging.ERROR)
    
    # Keep application loggers at specified level
    app_loggers = [
        'EventHubReader',
        'GenericEventHubMonitor',
        'LatestMessageMonitor', 
        'NamespaceDiscoveryFunction',
        'EventHubMonitorLogger'
    ]
    
    for logger_name in app_loggers:
        logging.getLogger(logger_name).setLevel(app_log_level)


def suppress_azure_logs():
    """
    Quick function to suppress Azure library logs without full configuration
    Use this if you already have logging configured elsewhere
    """
    configure_logging()


# Auto-configure when imported (can be disabled by setting environment variable)
if not os.getenv('SKIP_AUTO_LOG_CONFIG'):
    configure_logging(os.getenv('LOG_LEVEL', 'INFO'))