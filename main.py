from manipulate import manipulate
from fetch_redshift_func import run_redshift_query
from qi_bookings import main as bookings_main
from init_pull_push import main
from fetch_csv import main as main_fetch_csv
from send_update_slack import send_simple_slack_message

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


list_name = "Quality Inspection"

universal_path = r"C:/Users/SupplierCommunicatio/OneDrive - Razor HQ GmbH & Co. KG/Razor - Chetan_Locale/Procurement Trackers"
# universal_path = r"C:/Users/ChetanPaliwal/OneDrive - Razor HQ GmbH & Co. KG/Razor - Procurement Trackers"

df1 = main_fetch_csv(list_name)

def load_query(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

query = load_query("Queries/otif_dash.txt")

df2 = run_redshift_query(query, "otif_dash")

df2_manipulated = manipulate(df2, universal_path) ## process specific result

bookings_main(df2, df1, universal_path)

main(list_name, df1, df2_manipulated)

send_simple_slack_message("QI List Pull-Push Successful")

import main_otif