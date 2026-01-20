Multi-Event Hub Monitoring Solution
This approach uses a combination of:

Dynamic Event Hub Trigger - Monitors all Event Hubs
Timer-triggered Scanner - Discovers and monitors all consumer groups across all Event Hubs

Event Hub Namespace
├── Event Hub 1
│   ├── Consumer Group A → Monitor
│   ├── Consumer Group B → Monitor
│   └── $Default → Monitor
├── Event Hub 2
│   ├── Consumer Group C → Monitor
│   └── $Default → Monitor
└── Event Hub N
    └── All Consumer Groups → Monitor

↓ Monitoring Strategy ↓

1. Timer Function (Every 5 min)
   → Discovers all Event Hubs
   → Discovers all Consumer Groups
   → Logs checkpoint & lag info

2. Dynamic Event Hub Triggers
   → One function per Event Hub (auto-scaled)
   → Logs all messages with metadata
