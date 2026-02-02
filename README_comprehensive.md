# Comprehensive EventHub Monitoring Solution

This enhanced solution provides detailed monitoring of Azure EventHub consumption patterns with rich data classes, comprehensive lag analysis, and structured logging.

## 🎯 Features

### Comprehensive Discovery & Monitoring
✅ **Namespace Discovery** - Automatically discovers all EventHubs in namespace  
✅ **Consumer Group Discovery** - Finds all consumer groups for each EventHub  
✅ **Partition Analysis** - Monitors all partitions with sequence numbers and timestamps  
✅ **Activity Monitoring** - Tracks partition activity and message flow patterns  
✅ **Health Scoring** - Provides health scores and status for consumer groups  
✅ **Parallel Processing** - Uses concurrent threads for fast monitoring  
✅ **Message Production Rates** - Tracks message production rates per partition  
✅ **Partition Activity Patterns** - Monitors which partitions are receiving messages  
✅ **EventHub Utilization** - Measures total available messages and throughput  
✅ **System Health Monitoring** - Verifies message flow and system responsiveness  

### Rich Data Classes & Structured Logging
✅ **Data Classes** - Type-safe data classes for all metrics  
✅ **Structured JSON Logs** - Application Insights ready logging  
✅ **Comprehensive Metrics** - Namespace, EventHub, partition, and consumer group metrics  
✅ **Activity Alerts** - Automatic alerts for inactive or erroring partitions  
✅ **Performance Tracking** - Processing time and efficiency metrics  

### Advanced Analytics
✅ **KQL Queries** - 10+ pre-built queries for Application Insights  
✅ **Activity Dashboards** - Real-time activity and health monitoring  
✅ **Trend Analysis** - Time-series analysis of message flow patterns  
✅ **Error Detection** - Comprehensive error tracking and alerting  

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    EventHub Namespace                           │
├─────────────────────────────────────────────────────────────────┤
│  EventHub 1          │  EventHub 2          │  EventHub N      │
│  ├─ Partition 0      │  ├─ Partition 0      │  ├─ Partition 0  │
│  ├─ Partition 1      │  ├─ Partition 1      │  ├─ Partition 1  │
│  └─ Partition N      │  └─ Partition N      │  └─ Partition N  │
│                      │                      │                  │
│  Consumer Groups:    │  Consumer Groups:    │  Consumer Groups:│
│  ├─ $Default         │  ├─ $Default         │  ├─ $Default     │
│  ├─ cg-app-1         │  ├─ cg-service-2     │  ├─ cg-analytics │
│  └─ cg-monitor       │  └─ cg-backup        │  └─ cg-audit     │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│              Comprehensive Monitor                              │
├─────────────────────────────────────────────────────────────────┤
│  ├─ Namespace Discovery (Management API)                       │
│  ├─ EventHub Discovery (Parallel Processing)                   │
│  ├─ Consumer Group Discovery (Per EventHub)                    │
│  ├─ Partition Metrics Collection                               │
│  └─ Activity Analysis & Health Scoring                         │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                Structured Logging Output                       │
├─────────────────────────────────────────────────────────────────┤
│  📊 NamespaceComprehensiveMetrics                              │
│  🎯 EventHubComprehensiveMetrics                               │
│  👥 ConsumerGroupActivityDetailed                              │
│  🏥 ActivityAlerts                                             │
│  📈 MonitoringSummary                                          │
│     │                                                         │
│     ▼                                                         │
│  Application Insights → KQL Queries → Dashboards & Alerts    │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Data Classes

### Core Metrics Classes

```python
@dataclass
class PartitionMetrics:
    namespace: str
    hub_name: str
    partition_id: str
    beginning_sequence_number: Optional[int] = None
    last_enqueued_sequence_number: Optional[int] = None
    last_enqueued_offset: Optional[str] = None
    last_enqueued_time_utc: Optional[datetime] = None
    is_empty: bool = True
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # New calculated properties
    total_available_messages: Optional[int] = property
    retention_age_hours: Optional[float] = property
    has_recent_activity: bool = property

@dataclass
class ConsumerGroupLag:
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
    lag_status: str = property  # ACTIVE, EMPTY, ERROR
    is_healthy: bool = property
```

## 🚀 Usage

### 1. Local Testing

```bash
# Set required environment variables
export EventHubConnectionString='Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key'

# Set optional variables for full functionality
export SubscriptionId='your-subscription-id'
export ResourceGroup='your-resource-group'

# Run comprehensive monitoring test
python test_comprehensive_monitor.py

# Or run the exploration script directly
python LatestMessageMonitor/ehubExplore.py
```

### 2. Azure Function Deployment

The `ComprehensiveMonitorFunction` runs every 5 minutes and provides comprehensive monitoring:

```json
{
  "scriptFile": "__init__.py", 
  "bindings": [
    {
      "name": "timer",
      "type": "timerTrigger", 
      "direction": "in",
      "schedule": "0 */5 * * * *"
    }
  ],
  "timeout": "00:10:00"
}
```

### 3. Application Settings

Configure these in your Azure Function App:

| Setting | Required | Description |
|---------|----------|-------------|
| `EventHubConnectionString` | ✅ | EventHub connection string |
| `SubscriptionId` | ⚪ | Azure subscription ID (for Management API) |
| `ResourceGroup` | ⚪ | Resource group name (for Management API) |
| `LOG_LEVEL` | ⚪ | Logging level (default: INFO) |

## 📈 Monitoring Output

### Structured Log Types

#### 1. Namespace Overview
```json
{
  "eventType": "NamespaceComprehensiveMetrics",
  "timestamp": "2026-01-30T10:00:00Z",
  "namespace": "my-eventhub-ns",
  "discoveryMethod": "management_api",
  "totals": {
    "eventhubs": 3,
    "partitions": 12, 
    "consumerGroups": 8
  },
  "eventhubs": [...]
}
```

#### 2. Consumer Group Activity Details
```json
{
  "eventType": "ConsumerGroupActivityDetailed",
  "namespace": "my-eventhub-ns",
  "hubName": "orders-hub",
  "consumerGroup": "order-processor",
  "healthScore": 85.5,
  "aggregates": {
    "activePartitions": 3,
    "totalPartitions": 4,
    "emptyPartitions": 1,
    "errorPartitions": 0
  },
  "partitionDetails": [...]
}
```

#### 3. Activity Alerts
```json
{
  "eventType": "ActivityAlerts",
  "namespace": "my-eventhub-ns", 
  "alertCount": 2,
  "alerts": [
    {
      "severity": "WARNING",
      "eventhub": "payments-hub",
      "consumerGroup": "payment-processor",
      "healthScore": 45.0,
      "issue": "Multiple partitions showing errors"
    }
  ]
}
```

## 🔍 Application Insights Queries

### Quick Health Check
```kql
traces
| where timestamp > ago(30m)
| where message contains "Consumer Group Activity Detailed"
| extend logData = parse_json(substring(message, indexof(message, "{")))
| where todouble(logData.healthScore) < 70
| project 
    timestamp,
    hubName = logData.hubName,
    consumerGroup = logData.consumerGroup,
    healthScore = logData.healthScore,
    activePartitions = logData.aggregates.activePartitions,
    totalPartitions = logData.aggregates.totalPartitions
| order by healthScore asc
```

### Activity Trend Analysis
```kql
traces
| where timestamp > ago(4h)
| where message contains "Consumer Group Activity Detailed"
| extend logData = parse_json(substring(message, indexof(message, "{")))
| project 
    timestamp,
    hubName = tostring(logData.hubName),
    consumerGroup = tostring(logData.consumerGroup),
    activePartitions = tolong(logData.aggregates.activePartitions),
    totalPartitions = tolong(logData.aggregates.totalPartitions)
| summarize AvgActivePartitions = avg(activePartitions), 
    ActivityRate = (avg(activePartitions) * 100.0 / avg(totalPartitions))
    by hubName, consumerGroup, bin(timestamp, 15m)
| render timechart
```

## 🚨 Health & Alerting

### Health Score Calculation
- **90-100%**: ✅ Healthy (all partitions active, no errors)
- **70-89%**: ⚠️ Warning (some partitions empty or minor issues)
- **0-69%**: ❌ Critical (multiple errors or most partitions inactive)

### Automated Alerts
- Consumer groups with health score < 70%
- Partitions with connection errors
- EventHubs with no recent activity
- Processing failures and timeouts

### Alert Categories
- 🔴 **CRITICAL**: Health < 50% or multiple partition errors
- ⚠️ **WARNING**: Health 50-69% or inactive partitions
- 📊 **INFO**: Activity monitoring data and trends

## 🛠️ Troubleshooting

### Common Issues

1. **No EventHubs Discovered**
   - Check EventHub connection string
   - Verify namespace exists and is accessible
   - Set SubscriptionId and ResourceGroup for Management API

2. **Limited Consumer Group Discovery**
   - Management API not available → only $Default consumer group found
   - Set SubscriptionId, ResourceGroup for full discovery

3. **Partition Connection Errors**
   - Network connectivity issues to EventHub
   - Invalid EventHub connection string
   - EventHub namespace not accessible

4. **High Processing Time**
   - Too many EventHubs/consumer groups
   - Network latency to EventHub
   - Reduce max_workers or implement throttling

### Performance Optimization

- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent monitoring
- **Client Caching**: Reuses EventHub clients to avoid connection overhead
- **Efficient Partition Scanning**: Optimized partition property retrieval
- **Configurable Timeouts**: Optimized for fast execution

## 📚 File Structure

```
ehub-monitoring-app/
├── utils/
│   ├── comprehensive_monitor.py     # Core monitoring classes & logic
│   ├── logging_config.py           # Centralized logging setup
│   └── ...                         # Existing utilities
├── ComprehensiveMonitorFunction/
│   ├── __init__.py                 # Azure Function implementation
│   └── function.json              # Function configuration
├── LatestMessageMonitor/
│   └── ehubExplore.py             # Enhanced exploration script
├── kql/
│   └── comprehensive_monitoring.kql # 10+ KQL queries
├── test_comprehensive_monitor.py   # Local testing script
├── requirements.txt               # Updated dependencies
└── README_Comprehensive.md       # This documentation
```

## 🔄 Migration from Existing Solution

This comprehensive solution is fully backward compatible and enhances your existing monitoring:

1. **Existing Functions Continue Working** - No breaking changes
2. **Enhanced Logging** - Adds structured data classes and rich metrics
3. **New Timer Function** - `ComprehensiveMonitorFunction` for detailed analysis
4. **Improved Discovery** - Better EventHub and consumer group discovery
5. **Advanced Analytics** - KQL queries for deep insights

You can deploy the comprehensive solution alongside your existing functions for a gradual migration.

---

## 🎉 What You Get

With this comprehensive EventHub monitoring solution, you now have:

✅ **Complete Visibility** - Every EventHub, partition, and consumer group monitored  
✅ **Rich Metrics** - Sequence numbers, offsets, timestamps, activity analysis  
✅ **Health Insights** - Automated health scoring and alerting  
✅ **Operational Excellence** - Structured logging for Application Insights  
✅ **Advanced Analytics** - KQL queries for deep operational insights  
✅ **Production Ready** - Parallel processing, error handling, performance optimized  

Your monitoring data is now production-ready for:
- Real-time operational dashboards
- Automated alerting and incident response  
- Capacity planning and performance optimization
- Compliance and audit reporting
- Troubleshooting and root cause analysis
