import json
import os
from process_list import process_dataframes
from batch_with_permissions import main as main_batch
import warnings
import pandas as pd
import re

warnings.filterwarnings("ignore")

def load_dict(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main(list_name, df1, df2):

    if list_name == "OTIF Team Trackers":
        column_map = load_dict("Mappings/column_mapping_otif.json")
        user_id_map = load_dict("Mappings/user_id_mapping.json")
        always_add_user_ids = list(load_dict("Mappings/admin_user_id_mapping.json").values())
        identity_columns = ["field_7", "field_63"]

        upload_df = process_dataframes(df1, df2, column_map, user_id_map, always_add_user_ids, 'Yes', identity_columns)

        main_batch(upload_df, list_name)

    elif list_name == "Quality Inspection":
        column_map = load_dict("Mappings/column_mapping_qi.json")
        user_id_map = load_dict("Mappings/user_id_mapping.json")
        always_add_user_ids = list(load_dict("Mappings/admin_user_id_mapping_qi.json").values())
        identity_columns = ["final_poc", "sm"]

        upload_df = process_dataframes(df1, df2, column_map, user_id_map, always_add_user_ids, 'No', identity_columns)

        main_batch(upload_df, list_name)
