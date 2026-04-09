# Procurement QI & OTIF Pipeline

A Python data pipeline for managing Quality Inspection (QI) and On-Time-In-Full (OTIF) metrics in procurement workflows. Fetches data from SharePoint and Redshift, processes and normalizes it according to business rules, and syncs updates back to SharePoint with proper permission management.

## Key Features

- **Multi-source Data Integration**: Pulls QI data from SharePoint lists and OTIF metrics from Redshift
- **Data Processing & Transformation**: Normalizes values, handles data type conversions, and applies business logic rules
- **SharePoint Sync**: Batch creates/updates list items with automatic system column filtering and permission management
- **Email Scheduling**: Schedules QI emails based on Planned Receipt Date (PRD) and compliance tracking
- **Slack Notifications**: Sends real-time pipeline execution status updates
- **Excel & CSV Export**: Generates booking reports and data exports for analysis

## Tech Stack

- **Python 3** with pandas and NumPy for data manipulation
- **Redshift** for OTIF data warehouse queries
- **SharePoint** (via REST API) for list management and permissions
- **Slack SDK** for notifications
- **openpyxl** for Excel operations

## Setup

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd <repo-name>
   ```

2. **Create and activate virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure paths and credentials**
   - Update SharePoint and OneDrive paths in scripts (currently hardcoded in main.py)
   - Set Slack webhook URL for notifications
   - Configure Redshift connection credentials

## Usage

Run the main orchestration script:

```bash
python main.py
```

This will:
1. Fetch QI data from SharePoint
2. Query OTIF metrics from Redshift
3. Process and manipulate both datasets
4. Update QI bookings in Excel
5. Push processed data back to SharePoint with permissions
6. Send a Slack confirmation message

## Project Structure

- `main.py` - Main orchestration script
- `manipulate.py` - QI data processing and email scheduling logic
- `manipulate_otif.py` - OTIF data filtering and tracking
- `process_list.py` - DataFrame processing, column mapping, and comparison
- `batch_with_permissions.py` - SharePoint batch operations with permission handling
- `qi_bookings.py` - QI booking Excel operations
- `Queries/` - SQL queries for Redshift extraction
- `Mappings/` - Vendor email mapping and configuration files
- `Export/` & `Pull/` - Data output directories