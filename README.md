# OTIF Tracker

A Python-based supply chain data pipeline for tracking On-Time In-Full (OTIF) metrics, managing quality inspections, and synchronizing data between SharePoint, Redshift, and Excel systems. This tool extracts procurement data, processes KPIs, manages compliance workflows, and automates quality inspection email scheduling with Slack notifications.

## Features

- **Data Integration**: Pulls quality inspection and shipment data from SharePoint lists and Redshift data warehouse
- **OTIF Analysis**: Processes and tracks On-Time In-Full metrics across the supply chain
- **Batch Export**: Exports data to SharePoint in batches with granular permission management
- **Compliance Automation**: Schedules and tracks quality inspection emails based on PRD (Planned Receipt Date) windows
- **Multi-format Support**: Works with CSV, Excel, and JSON data formats
- **Slack Notifications**: Sends pipeline status updates to Slack channels
- **Data Validation**: Normalizes and validates data types across multiple sources

## Tech Stack

- **Language**: Python 3
- **Data Processing**: pandas, numpy
- **Data Warehouse**: Amazon Redshift
- **APIs**: SharePoint REST API, Slack SDK
- **Excel**: openpyxl
- **HTTP**: requests

## Setup

1. **Clone and create virtual environment**:
   ```bash
   git clone <repo-url>
   cd otif-tracker
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure paths and credentials**:
   - Set `universal_path` in `main.py` to your OneDrive/SharePoint Procurement Trackers location
   - Configure environment variables for:
     - Redshift connection details
     - SharePoint authentication
     - Slack webhook/token

4. **Prepare SQL queries**:
   - Place Redshift queries in the `Queries/` directory as `.txt` files
   - Example: `Queries/otif_dash.txt`

5. **Set up mappings**:
   - Create vendor email mappings in `Mappings/vendor_email_map.xlsx`

## Usage

Run the main pipeline:
```bash
python main.py
```

This will:
1. Fetch the Quality Inspection list from SharePoint
2. Query OTIF metrics from Redshift
3. Process and manipulate the data
4. Update quality inspection bookings in Excel
5. Sync changes back to SharePoint with proper permissions
6. Schedule compliance emails based on PRD dates
7. Send Slack notification on completion

### Individual Modules

- **`batch_with_permissions.py`**: Export data to SharePoint in batches with permission control
- **`qi_bookings.py`**: Process and update quality inspection bookings
- **`manipulate_otif.py`**: Filter and track OTIF-relevant shipments
- **`process_list.py`**: Normalize and validate data across sources