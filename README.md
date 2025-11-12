# Icinga2 to Splunk Integration

A Python-based integration that streams monitoring data from Icinga2 to Splunk using Icinga2 Event Streams and Splunk HTTP Event Collector (HEC).

## Features

- **Real-time Event Streaming**: Uses Icinga2 Event Streams API for instant data delivery
- **Comprehensive Event Types**: Captures CheckResults, StateChanges, Notifications, Acknowledgements, Comments, and Downtimes
- **Performance Data**: Parses and sends detailed performance metrics with thresholds
- **Separate Indexes**: Events and performance data are stored in separate Splunk indexes for better organization
- **Automatic Reconnection**: Handles network interruptions with exponential backoff
- **Batching**: Efficient event batching to reduce network overhead
- **Flexible Configuration**: Supports both JSON config files and environment variables

## Architecture

\`\`\`
Icinga2 API (Event Streams)
↓
Event Stream Client
↓
Event Handlers
↓
Splunk HEC Client (with batching)
↓
Splunk Indexes
├── icinga2_events (state changes, notifications, etc.)
└── icinga2_perfdata (performance metrics)
\`\`\`

## Prerequisites

- Icinga2 with API enabled
- Splunk Enterprise or Splunk Cloud with HEC enabled
- Python 3.6 or higher
- Network connectivity between Icinga2 and Splunk

## Installation

### 1. Clone or download this repository

\`\`\`bash
git clone <repository-url>
cd icinga2-splunk-integration
\`\`\`

### 2. Run the installation script

\`\`\`bash
sudo ./scripts/install.sh
\`\`\`

This will:

- Install Python dependencies
- Copy files to `/usr/local/bin/icinga2_splunk/`
- Create configuration file at `/etc/icinga2/splunk_config.json`
- Install systemd service

### 3. Configure Icinga2 API

Ensure the Icinga2 API is enabled and you have credentials:

\`\`\`bash
icinga2 api setup
\`\`\`

Create an API user in `/etc/icinga2/conf.d/api-users.conf`:

\`\`\`
object ApiUser "splunk" {
password = "your-secure-password"
permissions = [ "events/*" ]
}
\`\`\`

Restart Icinga2:
\`\`\`bash
systemctl restart icinga2
\`\`\`

### 4. Configure Splunk HEC

In Splunk:

1. Go to **Settings > Data Inputs > HTTP Event Collector**
2. Click **New Token**
3. Configure:
   - Name: `Icinga2 Integration`
   - Source type: `Automatic`
   - Allowed Indexes: Create or select `icinga2_events` and `icinga2_perfdata`
4. Save and copy the token

Create the indexes if they don't exist:
\`\`\`
Settings > Indexes > New Index

- Index Name: icinga2_events
- Index Name: icinga2_perfdata
  \`\`\`

### 5. Configure the Integration

Edit `/etc/icinga2/splunk_config.json`:

\`\`\`json
{
"icinga2_api_url": "https://localhost:5665",
"icinga2_api_username": "splunk",
"icinga2_api_password": "your-secure-password",
"icinga2_api_verify_ssl": false,

"splunk_hec_url": "https://splunk.example.com:8088/services/collector",
"splunk_hec_token": "your-hec-token-here",
"splunk_verify_ssl": true,
"splunk_index_events": "icinga2_events",
"splunk_index_perfdata": "icinga2_perfdata"
}
\`\`\`

### 6. Test the Connection

\`\`\`bash
/usr/local/bin/icinga2_splunk/test_connection.py
\`\`\`

### 7. Start the Service

\`\`\`bash
sudo systemctl enable icinga2-splunk
sudo systemctl start icinga2-splunk
sudo systemctl status icinga2-splunk
\`\`\`

## Configuration Options

### Icinga2 Settings

| Option                    | Description                                | Default                  |
| ------------------------- | ------------------------------------------ | ------------------------ |
| `icinga2_api_url`         | Icinga2 API URL                            | `https://localhost:5665` |
| `icinga2_api_username`    | API username                               | -                        |
| `icinga2_api_password`    | API password                               | -                        |
| `icinga2_api_verify_ssl`  | Verify SSL certificates                    | `false`                  |
| `icinga2_api_client_cert` | Client certificate path (alternative auth) | `null`                   |
| `icinga2_api_client_key`  | Client key path (alternative auth)         | `null`                   |

### Event Stream Settings

| Option                | Description                    | Default                 |
| --------------------- | ------------------------------ | ----------------------- |
| `event_stream_queue`  | Queue name for event stream    | `splunk-integration`    |
| `event_stream_types`  | List of event types to capture | See config.json.example |
| `event_stream_filter` | Optional filter expression     | `null`                  |

### Splunk Settings

| Option                  | Description                 | Default            |
| ----------------------- | --------------------------- | ------------------ |
| `splunk_hec_url`        | Splunk HEC endpoint         | -                  |
| `splunk_hec_token`      | HEC authentication token    | -                  |
| `splunk_verify_ssl`     | Verify SSL certificates     | `true`             |
| `splunk_index_events`   | Index for event data        | `icinga2_events`   |
| `splunk_index_perfdata` | Index for performance data  | `icinga2_perfdata` |
| `splunk_batch_size`     | Events per batch            | `100`              |
| `splunk_batch_interval` | Max seconds between batches | `5`                |

## Splunk Queries

### View all events

```spl
index=icinga2_events OR index=icinga2_perfdata
| table _time event_type host service state output
| sort -_time

```
