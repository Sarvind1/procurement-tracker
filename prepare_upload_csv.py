from manipulate import manipulate
from fetch_redshift_func import run_redshift_query
from qi_bookings import main as bookings_main
from fetch_csv import main as main_fetch_csv
from process_list import process_dataframes
import json


def load_query(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def load_dict(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def prepare_qi_upload_csv():
    """
    Prepares Quality Inspection list data and saves ready-to-upload CSV to Export/processed.csv
    Returns the processed dataframe ready for SharePoint upload
    """
    list_name = "Quality Inspection"
    universal_path = "."

    # Fetch current SharePoint list data
    df1 = main_fetch_csv(list_name)

    # Run Redshift query
    query = load_query("Queries/otif_dash.txt")
    df2 = run_redshift_query(query, "otif_dash")

    # Process data
    df2_manipulated = manipulate(df2, universal_path)

    # Skip bookings processing (QI email sending - not needed for CSV generation)
    # bookings_main(df2, df1, universal_path)

    # Load mappings
    column_map = load_dict("Mappings/column_mapping_qi.json")
    user_id_map = load_dict("Mappings/user_id_mapping.json")
    always_add_user_ids = list(load_dict("Mappings/admin_user_id_mapping_qi.json").values())
    identity_columns = ["final_poc", "sm"]

    # Process and prepare upload dataframe
    upload_df = process_dataframes(
        df1,
        df2_manipulated,
        column_map,
        user_id_map,
        always_add_user_ids,
        'No',
        identity_columns
    )

    print(f"\nCSV ready for upload at: Export/processed.csv")
    print(f"Total rows to process: {len(upload_df)}")
    print(f"Actions breakdown:")
    print(upload_df['action'].value_counts())

    return upload_df


if __name__ == "__main__":
    prepare_qi_upload_csv()
