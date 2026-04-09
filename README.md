# Procurement Tracker

An automated procurement tracking system that orchestrates data pipelines across Redshift, SharePoint, and Slack to manage On-Time In-Full (OTIF) metrics, Quality Inspection (QI) scheduling, and supplier communications for Razor HQ's procurement operations.

## Features

- **Multi-source data integration**: Fetches data from AWS Redshift, SharePoint lists, and Excel workbooks
- **OTIF tracking**: Monitors on-time and in-full delivery metrics with automated status filtering and process categorization
- **Quality Inspection automation**: Schedules QI emails based on PRD (Probable Request Dates) with vendor mapping
- **SharePoint batch operations**: Bulk creates and manages SharePoint list items with permission handling
- **Slack notifications**: Sends automated pipeline status updates to Slack
- **Data transformation**: Processes, cleans, and normalizes complex procurement datasets with format conversion and validation

## Tech Stack

- **Language**: Python 3
- **Data Processing**: Pandas, NumPy
- **Database**: AWS Redshift
- **APIs**: SharePoint REST API, Slack SDK
- **Spreadsheets**: OpenPyXL (Excel)
- **Data**: CSV, JSON, XLSX

## Setup

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   - `AWS_ACCESS_KEY_ID` - AWS credentials for Redshift
   - `AWS_SECRET_ACCESS_KEY` - AWS secret key
   - `SLACK_BOT_TOKEN` - Slack app token
   - SharePoint credentials (site URL, username, password)

4. Update paths in `main.py`:
   - Set `universal_path` to your OneDrive/SharePoint directory
   - Ensure `Queries/` and `Mappings/` directories exist locally

## Usage

Run the main orchestration pipeline:
```bash
python main.py
```

This will:
1. Fetch SharePoint list data for "Quality Inspection"
2. Query OTIF metrics from Redshift
3. Process and manipulate data (OTIF analysis, QI scheduling)
4. Push updates back to SharePoint lists with permissions
5. Send Slack notification on completion

Individual modules can be imported and used separately for specific operations (batch processing, data fetching, Slack messaging).

## Project Structure

- `main.py` - Main pipeline orchestrator
- `fetch_redshift_func.py` - Redshift query execution
- `fetch_csv.py` - SharePoint list data fetching
- `batch_with_permissions.py` - Bulk SharePoint operations
- `process_list.py` - Data validation and column mapping
- `manipulate.py` - QI email scheduling logic
- `manipulate_otif.py` - OTIF status tracking and filtering
- `qi_bookings.py` - QI booking Excel workbook generation
- `send_update_slack.py` - Slack notification utilities
- `init_pull_push.py` - SharePoint push operations
- `Queries/` - SQL query templates
- `Mappings/` - Configuration and vendor mappings
- `Export/` - Batch operation results
- `Pull/` - Downloaded SharePoint data