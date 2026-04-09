# Supply Chain OTIF Tracker

A Python-based system for managing On-Time In-Full (OTIF) metrics and Quality Inspection (QI) data in supply chain operations. Automates data synchronization between CSV sources, Redshift analytics, and SharePoint, with email scheduling and Slack notifications.

## Features

- **OTIF Tracking**: Monitor on-time and in-full delivery metrics across production statuses
- **Quality Inspection Management**: Automated QI email scheduling and booking coordination
- **SharePoint Sync**: Bidirectional data synchronization with permission-level user access control
- **Multi-Source Integration**: Aggregates data from CSV lists, Redshift SQL, and SharePoint
- **Slack Notifications**: Automated status updates and completion alerts
- **Batch Operations**: Efficient processing of large datasets with configurable column mappings

## Tech Stack

- **Python 3.7+**
- **pandas** - Data processing and manipulation
- **numpy** - Numerical operations
- **openpyxl** - Excel workbook handling
- **slack-sdk** - Slack message delivery
- **Redshift** - Analytics queries (configured via connection parameters)
- **SharePoint API** - List synchronization

## Setup

### Prerequisites

- Python 3.7 or higher
- Access to Redshift analytics database
- SharePoint site with appropriate permissions
- Slack workspace token (optional, for notifications)

### Installation

1. Clone the repository and create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your environment variables:
```bash
# Create .env file with:
REDSHIFT_HOST=your_redshift_host
REDSHIFT_DATABASE=your_db
REDSHIFT_USER=your_user
REDSHIFT_PASSWORD=your_password
SLACK_BOT_TOKEN=your_slack_token
SHAREPOINT_SITE_URL=your_site_url
```

4. Update file paths in the scripts:
   - Modify `universal_path` in `main.py` to match your OneDrive/SharePoint directory
   - Update query paths in `Queries/` directory

## Usage

### Run the main workflow:
```bash
python main.py
```

This will:
1. Fetch Quality Inspection list from SharePoint
2. Query OTIF metrics from Redshift
3. Process and manipulate the data
4. Sync updated data back to SharePoint with permissions
5. Schedule QI booking emails
6. Send completion notification to Slack

### Individual modules:
- `batch_with_permissions.py` - Direct SharePoint batch operations
- `manipulate_otif.py` - OTIF-specific data filtering and processing
- `qi_bookings.py` - Quality Inspection booking management

## File Structure

```
├── main.py                          # Main orchestration script
├── Queries/                         # SQL query files for Redshift
├── Export/                          # Generated export files (gitignored)
├── Pull/                            # Downloaded data files (gitignored)
├── Compliance/QI Email Scheduler/   # Email scheduling data (gitignored)
├── Mappings/                        # Configuration mappings
└── [Python modules]                 # Data processing functions
```

## Notes

- Hardcoded file paths are user-specific and should be moved to environment configuration
- Large data files (.csv, .xlsx) are excluded from version control
- Set up SharePoint app credentials for production use