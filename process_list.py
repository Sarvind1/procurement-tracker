import pandas as pd
import os
import json
from datetime import datetime
import re

def process_dataframes(df1, df2, column_mapping: dict, user_id_map: dict, always_add_user_ids: list, item_level_access, identity_columns):
    df1 = df1.drop_duplicates(subset=["Title"], keep="first").copy()

    # -----------------------
    # helpers
    # -----------------------
    def try_parse_json(x):
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                return x
        return x
    
    def apply_format_type(col, value):
        cfg = column_mapping.get(col, {})
        if cfg.get("format_type") == "float":
            try:
                if pd.isna(value):
                    return pd.NA
                return round(float(value), 3)
            except Exception:
                return value
        return value

    def normalize_value(v):
        if pd.isna(v):
            return pd.NA
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            s = v.strip()
            try:
                f = float(s)
                if f.is_integer():
                    return int(f)
                return f
            except Exception:
                return s
        if isinstance(v, float):
            if v.is_integer():
                return int(v)
            return v
        return v

    def equivalent(v1, v2):
        v1_na = pd.isna(v1) or (isinstance(v1, str) and v1.strip() == "")
        v2_na = pd.isna(v2) or (isinstance(v2, str) and v2.strip() == "")
        if v1_na and v2_na:
            return True
        if v1_na != v2_na:
            return False

        n1 = normalize_value(v1)
        n2 = normalize_value(v2)

        n1_na = pd.isna(n1) or (isinstance(n1, str) and n1.strip() == "")
        n2_na = pd.isna(n2) or (isinstance(n2, str) and n2.strip() == "")
        if n1_na and n2_na:
            return True
        if n1_na != n2_na:
            return False

        try:
            return n1 == n2
        except Exception:
            return False

    def str_to_id_set(s):
        if pd.isna(s):
            return set()
        if isinstance(s, (list, set, tuple)):
            return {str(x).strip() for x in s if x is not None and str(x).strip() != ""}
        ss = str(s).strip()
        if ss == "":
            return set()
        parts = [p.strip() for p in ss.replace(",", ";").split(";") if p.strip() != ""]
        return set(parts)

    def join_id_list(id_list):
        unique = []
        seen = set()
        for uid in id_list:
            if pd.isna(uid):
                continue
            s = str(uid)
            if s not in seen:
                seen.add(s)
                unique.append(s)
        return "; ".join(unique)

    # -----------------------
    # STEP 1 — align df2 to df1 using mapping
    # -----------------------
    df2_rename_map = {}
    for df1_col, cfg in column_mapping.items():
        tgt = cfg.get("target_col")
        if tgt:
            df2_rename_map[tgt] = df1_col

    df2_aligned = df2.rename(columns=df2_rename_map)

    if "Title" not in df2_aligned.columns:
        raise KeyError("df2 after renaming must contain Title")

    df2_original_idx = df2_aligned.set_index("Title")

    mapped_cols = list(column_mapping.keys())
    df1_cols = df1.columns.tolist()
    mandatory_cols = ["ID"]

    cols_needed = list(dict.fromkeys(df1_cols + mapped_cols + mandatory_cols))

    existing_cols = [c for c in cols_needed if c in df2_aligned.columns]
    df2_trimmed = df2_aligned[existing_cols].copy()

    for col in cols_needed:
        if col not in df2_trimmed.columns:
            df2_trimmed[col] = pd.NA

    # populate ID
    if "ID" in df1.columns:
        df2_trimmed["ID"] = df2_trimmed["Title"].map(df1.set_index("Title")["ID"]).astype("Int64")
    else:
        df2_trimmed["ID"] = pd.NA

    df2_result = df2_trimmed[cols_needed].copy()

    df2_later = df2_result.copy()

    # -----------------------
    # STEP 2 — action (add/delete/update)
    # -----------------------
    KEY = "Title"

    df2_result["action"] = ""

    df1_keys = set(df1[KEY].dropna().astype(str))
    df2_keys = set(df2_result[KEY].dropna().astype(str))

    delete_keys = df1_keys - df2_keys
    add_keys = df2_keys - df1_keys
    common_keys = df1_keys & df2_keys

    df2_result.loc[df2_result[KEY].isin(delete_keys), "action"] = "delete"
    df2_result.loc[df2_result[KEY].isin(add_keys), "action"] = "add"

    cols_to_check = [
        df1_col for df1_col, cfg in column_mapping.items()
        if cfg.get("update_check", "no").lower() == "yes"
    ]

    df1_idx = df1.set_index(KEY)
    df2_idx = df2_result.set_index(KEY)

    # -----------------------
    # STEP 2.5 — dropdown decode in df1
    # -----------------------
    for df1_col, cfg in column_mapping.items():
        if cfg.get("dropdown", "no").lower() == "yes" and df1_col in df1_idx.columns:
            df1_idx[df1_col] = df1_idx[df1_col].apply(try_parse_json)
            df1_idx[df1_col] = df1_idx[df1_col].apply(
                lambda x: x.get("Value") if isinstance(x, dict) and "Value" in x else x
            )

    # -----------------------
    # STEP 3 — update detection + blanking
    # -----------------------
    for key in common_keys:
        row1 = df1_idx.loc[key]
        row2 = df2_idx.loc[key]

        changed_short = []
        for col in cols_to_check:
            # v1 = row1.get(col, pd.NA)
            # v2 = row2.get(col, pd.NA)
            v1 = apply_format_type(col, row1.get(col, pd.NA))
            v2 = apply_format_type(col, row2.get(col, pd.NA))
            if not equivalent(v1, v2):
                changed_short.append(col)

        if changed_short:
            df2_result.loc[df2_result[KEY] == key, "action"] = "update"

            changed_full = []
            # for col in df2_result.columns:
            #     if col in {"Title", "ID", "action", "Add_UserIDs", "Remove_UserIDs"}:
            #         continue
            #     v1 = row1.get(col, pd.NA)
            #     v2 = df2_original_idx.loc[key].get(col, pd.NA) if col in df2_original_idx.columns else pd.NA
            #     if not equivalent(v1, v2):
            #         changed_full.append(col)

            for col in df2_result.columns:
                if col in {"Title", "ID", "action", "Add_UserIDs", "Remove_UserIDs"}:
                    continue

                v1_raw = row1.get(col, pd.NA)
                v2_raw = df2_original_idx.loc[key].get(col, pd.NA) if col in df2_original_idx.columns else pd.NA

                v1 = apply_format_type(col, v1_raw)
                v2 = apply_format_type(col, v2_raw)

                if not equivalent(v1, v2):
                    changed_full.append(col)

            # blank unchanged
            for col in df2_result.columns:
                if col in {"Title", "ID", "action", "Add_UserIDs", "Remove_UserIDs"}:
                    continue
                if col not in changed_full:
                    df2_result.loc[df2_result[KEY] == key, col] = pd.NA

            # keep df2's original values for changed cols
            for col in changed_full:
                # df2_result.loc[df2_result[KEY] == key, col] = df2_original_idx.loc[key].get(col)
                val = df2_original_idx.loc[key].get(col)
                df2_result.loc[df2_result[KEY] == key, col] = apply_format_type(col, val)


    # -----------------------
    # STEP 4 — generate Add_UserIDs and Remove_UserIDs
    # -----------------------
    df2_result["Add_UserIDs"] = ""
    df2_result["Remove_UserIDs"] = ""
    df2_result["ids_with_access"] = ""

    if item_level_access=='Yes':
        identity_columns = identity_columns

        for key in df2_result[KEY].astype(str).unique():
            if key not in df1_idx.index or key not in df2_original_idx.index:
                continue

            add_list = []
            remove_list = []

            for col in identity_columns:
                new = df1_idx.loc[key].get(col, pd.NA)
                old = df2_original_idx.loc[key].get(col, pd.NA)

                if equivalent(new, old):
                    if old is not None and not pd.isna(old) and str(old).strip() != "":
                        uid_old = user_id_map.get(str(old))
                        if uid_old: add_list.append(uid_old)
                else:
                    if old is not None and not pd.isna(old) and str(old).strip() != "":
                        uid_old = user_id_map.get(str(old))
                        if uid_old: add_list.append(uid_old)
                    if new is not None and not pd.isna(new) and str(new).strip() != "":
                        uid_new = user_id_map.get(str(new))
                        if uid_new: remove_list.append(uid_new)

            df2_result.loc[df2_result[KEY] == key, "Add_UserIDs"] = join_id_list(add_list)
            df2_result.loc[df2_result[KEY] == key, "Remove_UserIDs"] = join_id_list(remove_list)

        

        # '''
        # df2["final_new_ids"]
        # df1["ids_with_access"]

        # set make them both

        # left only in df1 → remove
        # right only in df2 → add
        
        # '''

        # -----------------------
        # STEP 4.2 — always append fixed Add_UserIDs
        # -----------------------
        if always_add_user_ids:
            always_str = "; ".join(str(uid) for uid in always_add_user_ids)
            def append_always(x):
                xs = "" if pd.isna(x) else str(x)
                if xs.strip() == "":
                    return always_str
                return xs + "; " + always_str
            df2_result["Add_UserIDs"] = df2_result["Add_UserIDs"].apply(append_always)

        df2_result["Add_UserIDs"] = df2_result["Add_UserIDs"].str.replace(r";\s*", "; ", regex=True)
        df2_result["Remove_UserIDs"] = df2_result["Remove_UserIDs"].str.replace(r";\s*", "; ", regex=True)

        # -----------------------
        # STEP 4.3 — duplicate Add_UserIDs → ids_with_access
        # -----------------------
        df2_result["ids_with_access"] = df2_result["Add_UserIDs"]

        # -----------------------
        # STEP 4.4 — CLEAN ACCESS LISTS BASED ON df1["ids_with_access"]
        # -----------------------
        if "ids_with_access" in df1.columns:
            df1_access_map = {k: str_to_id_set(v) for k, v in df1.set_index(KEY)["ids_with_access"].items()}

            for key in df2_result[KEY].astype(str).unique():
                if key not in df1_access_map:
                    continue

                existing_access = df1_access_map[key]

                add_str = df2_result.loc[df2_result[KEY] == key, "Add_UserIDs"].iloc[0]
                rem_str = df2_result.loc[df2_result[KEY] == key, "Remove_UserIDs"].iloc[0]

                add_set = str_to_id_set(add_str)
                rem_set = str_to_id_set(rem_str)

                # Rule 1: Remove users from add_set who already have access
                corrected_add = add_set - existing_access

                # Rule 2: Keep only users in remove_set that actually have access
                corrected_remove = rem_set & existing_access

                df2_result.loc[df2_result[KEY] == key, "Add_UserIDs"] = join_id_list(sorted(corrected_add))
                df2_result.loc[df2_result[KEY] == key, "Remove_UserIDs"] = join_id_list(sorted(corrected_remove))

        if "ids_with_access" in df1.columns:
            df1_access_map = {k: str_to_id_set(v) for k, v in df1.set_index(KEY)["ids_with_access"].items()}

            for key in common_keys:
                if df2_result.loc[df2_result[KEY] == key, "action"].iloc[0] != "update":
                    continue

                curr = df1_access_map.get(key, set())

                add_set = str_to_id_set(df2_result.loc[df2_result[KEY] == key, "Add_UserIDs"].iloc[0])
                rem_set = str_to_id_set(df2_result.loc[df2_result[KEY] == key, "Remove_UserIDs"].iloc[0])

                add_new = add_set - curr
                rem_existing = rem_set & curr

                # Check if ANY data columns changed (other than access fields)
                row = df2_result.loc[df2_result[KEY] == key].iloc[0]
                data_changed = False
                for col in cols_to_check:
                    if not pd.isna(row[col]):   # if df2_result kept a value here, it's a change
                        data_changed = True
                        break

                # Only cancel update if NO access changes AND NO data changes
                if not add_new and not rem_existing and not data_changed:
                    df2_result.loc[df2_result[KEY] == key, "action"] = ""

    else:
        # When item_level_access is 'No', cancel updates with no data changes
        for key in common_keys:
            rows_matching = df2_result[df2_result[KEY] == key]
            if len(rows_matching) == 0:
                continue

            current_action = rows_matching["action"].iloc[0]
            if current_action != "update":
                continue

            # Check if ANY data columns changed
            row = rows_matching.iloc[0]
            data_changed = False
            for col in cols_to_check:
                val = row[col]
                # Treat pd.NA, None, empty string, and string "nan"/"NaN"/"NAN" as empty
                if pd.isna(val):
                    continue
                if isinstance(val, str) and val.strip() in ["", "nan", "NaN", "NAN"]:
                    continue
                # If we get here, there's a real value - it's a change
                data_changed = True
                break

            # Cancel update if NO data changes
            if not data_changed:
                df2_result.loc[df2_result[KEY] == key, "action"] = ""

    # -----------------------
    # STEP 5 — INSERT DELETE ROWS
    # -----------------------
    if delete_keys:
        delete_rows = df1[df1[KEY].astype(str).isin(delete_keys)].copy()
        delete_rows["action"] = "delete"
        delete_rows["Add_UserIDs"] = ""
        delete_rows["Remove_UserIDs"] = ""

        for col in df2_result.columns:
            if col not in delete_rows.columns:
                delete_rows[col] = pd.NA

        delete_rows = delete_rows[df2_result.columns]
        df2_result = pd.concat([df2_result, delete_rows], ignore_index=True)


    # -----------------------
    # STEP 6 — Remove rows where action is blank
    # -----------------------
    df2_result = df2_result[df2_result["action"].astype(str).str.strip() != ""].copy()

    df2_result["ids_with_access"] = df2_result["Add_UserIDs"]

    for col, cfg in column_mapping.items():
        if cfg.get("format_type") == "float" and col in df2_result.columns:
            df2_result[col] = df2_result[col].apply(lambda x: round(float(x), 3) if pd.notna(x) else x)

    cols_to_fill = df2_result.select_dtypes(include=["object", "string"]).columns

    df2_result[cols_to_fill] = (
        df2_result[cols_to_fill]
        .replace(["nan", "NaN", "NAN"], "")
        .fillna("")
    )

    FIELDS_TO_CHECK = ["field_63", "field_7"]
    KEY_COL = "Title"
    ADD_COL = "Add_UserIDs"

    # ---------- Step 1: Sync field_63 and field_7 from df2_later ----------
    if KEY_COL in df2_later.columns and KEY_COL in df2_result.columns:
        valid_fields = [
            c for c in FIELDS_TO_CHECK
            if c in df2_later.columns and c in df2_result.columns
        ]

        if valid_fields:
            lookup_df = df2_later[[KEY_COL] + valid_fields].set_index(KEY_COL)

            for col in valid_fields:
                mapped = df2_result[KEY_COL].map(lookup_df[col])
                df2_result[col] = df2_result[col].where(mapped.isna(), mapped)

    ALWAYS_ADD_IDS = {str(v) for v in always_add_user_ids}

    # ---------- Step 2: Build Add_UserIDs from field_63, field_7 + existing ----------
    def append_ids_from_fields(row):
        # 1️⃣ Existing Add_UserIDs
        existing_ids = str_to_id_set(row.get("Add_UserIDs", ""))

        # 2️⃣ IDs from field_63 & field_7
        new_ids = set()

        for col in FIELDS_TO_CHECK:
            val = row.get(col, pd.NA)
            if pd.isna(val):
                continue

            for v in str(val).replace(",", ";").split(";"):
                v = v.strip()
                if not v:
                    continue

                uid = user_id_map.get(v)
                if uid:
                    new_ids.add(str(uid))

        # 3️⃣ ALWAYS add global IDs
        final_ids = existing_ids | new_ids | ALWAYS_ADD_IDS

        return join_id_list(sorted(final_ids))

    df2_result[ADD_COL] = df2_result.apply(append_ids_from_fields, axis=1)


    # -----------------------
    # STEP 7 — save
    # -----------------------
    os.makedirs("Export", exist_ok=True)
    file_path = os.path.join("Export", "processed.csv")
    df2_result.to_csv(file_path, index=False, encoding="utf-8")

    print(f"\nProcessed saved to '{file_path}'")
    return df2_result
