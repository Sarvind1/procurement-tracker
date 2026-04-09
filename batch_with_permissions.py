import csv
import json
import requests
import time
import os
import math
from typing import List, Dict, Any

import numpy as np

# --- Configuration ---
EXPORT_DIR = "Export"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Columns that must never be sent to SharePoint
SYSTEM_COLUMNS = {
    "Action", "action", "ID", "Id", "ItemId", "ItemInternalId", "Modified", "Created",
    "Author", "Editor", "GUID", "VersionNumber", "{Identifier}",
    "{IsFolder}", "{Thumbnail}", "{Link}", "{Name}", "{FilenameWithExtension}",
    "{Path}", "{FullPath}", "{HasAttachments}", "{ContentType}",
    "{ContentType}#Id", "Add_UserIDs", "Remove_UserIDs", "@odata.etag"  # Permission columns
}


# -----------------------
# Helpers: blank / NaN handling
# -----------------------

def is_blank(v: Any) -> bool:
    """Return True if value is None, NaN, empty, whitespace, or string 'nan/null/none'."""
    if v is None:
        return True

    # numpy / pandas NaN
    try:
        # Covers floats like float('nan') and pandas/numpy NaN
        if isinstance(v, float) and math.isnan(v):
            return True
        # Direct numpy NaN sentinel
        if v is np.nan:
            return True
    except Exception:
        pass

    # Convert to string for final checks
    try:
        s = str(v).strip().lower()
    except Exception:
        return True

    if s in ("", "nan", "none", "null"):
        return True

    return False


def is_system_column(col: str) -> bool:
    """Detect SharePoint system columns that must not be sent."""
    if not col:
        return False
    return (
        col in SYSTEM_COLUMNS
        or col.startswith("{")
        or col.lower().startswith("odata")
        or col.endswith("Claims")
        or col.endswith("#Id")
        or col.endswith("#Claims")
        or col.endswith("#Value")
        or "@odata" in col
    )


# -----------------------
# Core payload cleaning/parsing
# -----------------------

def clean_payload(row: Dict[str, Any]) -> Dict[str, str]:
    """Remove SharePoint system/metadata fields + permission columns and blank values."""
    MAX_FIELD_LENGTH = 2000

    cleaned: Dict[str, str] = {}

    for k, v in row.items():
        # normalize key
        if not k:
            continue

        # filter blank/None/NaN early
        if is_blank(v):
            continue

        # skip system columns
        if is_system_column(k):
            continue

        # stringify safely
        try:
            v_str = str(v).strip()
        except Exception:
            continue

        # skip values that look like JSON objects (original logic kept)
        if v_str.startswith("{") and v_str.endswith("}"):
            continue

        # enforce max length
        if len(v_str) > MAX_FIELD_LENGTH:
            v_str = v_str[:MAX_FIELD_LENGTH]

        cleaned[k] = v_str

    return cleaned


def parse_user_ids(user_id_string: Any) -> List[str]:
    """Parse semicolon-separated user IDs with NaN handling."""
    if is_blank(user_id_string):
        return []
    return [uid.strip() for uid in str(user_id_string).split(";") if uid.strip()]


def get_action(row: Dict[str, Any]) -> str:
    """Get action from row, safe for NaN values. Returns lowercase action or empty string."""
    v = row.get("Action") or row.get("action")
    if is_blank(v):
        return ""
    return str(v).strip().lower()


def get_item_id(row: Dict[str, Any]) -> str:
    """Get ItemId from row, checking multiple possible column names and handling NaN."""
    v = row.get("ItemId") or row.get("ID") or row.get("Id")
    if is_blank(v):
        return ""
    return str(v).strip()


# -----------------------
# Batch generation helpers
# -----------------------

def generate_create_batch(rows: List[Dict[str, Any]], base_url: str, batch_number: int = 1):
    """
    Generate batch request for CREATE operations only (no permissions).
    Used as first batch for ADD items that need permissions.
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for row in rows:
        action = get_action(row)
        if action != "add":
            continue

        payload = clean_payload(row)

        output.append(f"--{changeset_id}")
        output.append("Content-Type: application/http")
        output.append("Content-Transfer-Encoding: binary")
        output.append("")
        output.append(f"POST {base_url} HTTP/1.1")
        output.append("Content-Type: application/json;odata=nometadata")
        output.append("Accept: application/json;odata=nometadata")
        output.append("")
        output.append(json.dumps(payload))
        output.append("")

        processed_count += 1

    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


def generate_permissions_batch(item_ids: List[str], user_permissions: List[Dict[str, List[str]]],
                               base_url: str, role_id: int, batch_number: int = 1):
    """
    Generate batch request for permission operations on existing items.
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for item_id, perms in zip(item_ids, user_permissions):
        add_users = perms.get('add_users', [])
        remove_users = perms.get('remove_users', [])

        # Only process if there are permissions to set
        if not add_users and not remove_users:
            continue

        # Break inheritance if adding users
        if add_users:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"POST {base_url}({item_id})/breakroleinheritance(copyRoleAssignments=false) HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("Content-Type: application/json;odata=verbose")
            output.append("")
            output.append("")

            # Add each user
            for user_id in add_users:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/addroleassignment(principalid=@p,roledefid=@r)?@p={user_id}&@r={role_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

        # Remove users
        for user_id in remove_users:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"POST {base_url}({item_id})/roleassignments/removeroleassignment(principalid=@p)?@p={user_id} HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("Content-Type: application/json;odata=verbose")
            output.append("")
            output.append("")

        processed_count += 1

    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


def generate_batch_with_permissions(rows: List[Dict[str, Any]], base_url: str, role_id: int, batch_number: int = 1):
    """
    Generate batch request for UPDATE/DELETE operations with permissions.
    (Used for operations where ItemId already exists)
    """
    batch_id = f"batch_{batch_number:04d}"
    changeset_id = f"changeset_{batch_number:04d}"

    output = []
    output.append(f"--{batch_id}")
    output.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    output.append("")

    processed_count = 0

    for row in rows:
        action = get_action(row)
        item_id = get_item_id(row)

        # Skip ADD actions - they're handled separately
        if action == "add":
            continue

        # Parse permission columns
        add_user_ids = parse_user_ids(row.get("Add_UserIDs", ""))
        remove_user_ids = parse_user_ids(row.get("Remove_UserIDs", ""))

        # Clean data payload (excludes permission columns)
        payload = clean_payload(row)

        if action == "update" and item_id:
            # Step 1: Update item
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"PATCH {base_url}({item_id}) HTTP/1.1")
            output.append("Content-Type: application/json;odata=nometadata")
            output.append("Accept: application/json;odata=nometadata")
            output.append("IF-MATCH: *")
            output.append("")
            output.append(json.dumps(payload))
            output.append("")

            # Step 2: Break inheritance if adding users
            if add_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/breakroleinheritance(copyRoleAssignments=false) HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            # Step 3: Add permissions
            for user_id in add_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/addroleassignment(principalid=@p,roledefid=@r)?@p={user_id}&@r={role_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            # Step 4: Remove permissions
            for user_id in remove_user_ids:
                output.append(f"--{changeset_id}")
                output.append("Content-Type: application/http")
                output.append("Content-Transfer-Encoding: binary")
                output.append("")
                output.append(f"POST {base_url}({item_id})/roleassignments/removeroleassignment(principalid=@p)?@p={user_id} HTTP/1.1")
                output.append("Accept: application/json;odata=nometadata")
                output.append("Content-Type: application/json;odata=verbose")
                output.append("")
                output.append("")

            processed_count += 1

        elif action == "delete" and item_id:
            output.append(f"--{changeset_id}")
            output.append("Content-Type: application/http")
            output.append("Content-Transfer-Encoding: binary")
            output.append("")
            output.append(f"DELETE {base_url}({item_id}) HTTP/1.1")
            output.append("Accept: application/json;odata=nometadata")
            output.append("IF-MATCH: *")
            output.append("")
            output.append("")

            processed_count += 1

    # Close boundaries
    output.append(f"--{changeset_id}--")
    output.append(f"--{batch_id}--")
    output.append("")

    return "\n".join(output), batch_id, processed_count


# -----------------------
# Input readers (CSV & DataFrame)
# -----------------------

def read_csv_in_chunks(csv_file_path: str, chunk_size: int = 100):
    """Read CSV file and yield chunks of rows (kept for backward compatibility)."""
    with open(csv_file_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def read_df_in_chunks(df, chunk_size: int = 100):
    """Yield chunks of a dataframe as list-of-dict rows."""
    # df is expected to be a pandas.DataFrame or any object supporting iloc and len
    total = len(df)
    for start in range(0, total, chunk_size):
        chunk_df = df.iloc[start:start + chunk_size]
        # convert to list of dict rows
        yield chunk_df.to_dict(orient="records")


# -----------------------
# Response parsing & HTTP
# -----------------------

def parse_batch_response_for_item_ids(response_data: Any):
    """
    Extract ItemIds from Power Automate/SharePoint batch response.

    Returns:
        List of item IDs created in the batch
    """
    try:
        if isinstance(response_data, dict):
            # Adapt this block to actual Power Automate response structure if needed
            if "itemIds" in response_data:
                item_ids_data = response_data.get("itemIds", [])
                return [item.get("Id") for item in item_ids_data if "Id" in item]
        return []
    except Exception as e:
        print(f"   ⚠️ Error parsing response for ItemIds: {e}")
        return []


def send_to_power_automate(batch_body: str, target_url: str, batch_boundary: str, pa_webhook_url: str, batch_num: int = 1):
    """Send the batch payload to Power Automate HTTP trigger."""
    payload = {
        "batchBody": batch_body,
        "targetUrl": target_url,
        "batchBoundary": batch_boundary
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(pa_webhook_url, json=payload, headers=headers, timeout=90)
        response.raise_for_status()

        result = response.json() if response.text else {}

        return {
            "success": True,
            "batch_number": batch_num,
            "status_code": response.status_code,
            "response": result
        }

    except requests.exceptions.Timeout:
        return {
            "success": True,
            "batch_number": batch_num,
            "status": "timeout"
        }

    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_msg = e.response.text
            except Exception:
                pass

        return {
            "success": False,
            "batch_number": batch_num,
            "error": error_msg
        }


# -----------------------
# Main processing: two-batch approach
# -----------------------

def process_df_with_permissions(df, base_url: str, role_id: int, pa_webhook_url: str,
                                chunk_size: int = 20, delay_between_batches: int = 2, max_batches: int = None):
    """
    Process a DataFrame with TWO-BATCH approach:
    - Batch 1: Create ADD items
    - Batch 2: Set permissions on newly created items
    - Single batch for UPDATE/DELETE with permissions
    """

    print(f"\n{'='*60}")
    print(f"Starting TWO-BATCH processing WITH PERMISSIONS (DATAFRAME MODE)")
    print(f"{'='*60}")
    print(f"Chunk Size: {chunk_size} rows per batch")
    print(f"Role ID for Add Access: {role_id}")
    print(f"Delay Between Batches: {delay_between_batches} seconds")
    if max_batches:
        print(f"Max Batches (TEST MODE): {max_batches}")
    print(f"{'='*60}\n")

    results = []
    batch_number = 1
    total_rows = len(df)
    total_processed = 0
    failed_batches = []
    timeout_batches = []

    for chunk in read_df_in_chunks(df, chunk_size):
        if max_batches and batch_number > max_batches:
            print(f"⚠ Reached max batch limit ({max_batches}). Stopping.\n")
            break

        # chunk is list[dict]
        # Separate ADD rows from UPDATE/DELETE rows
        add_rows = [r for r in chunk if get_action(r) == 'add']
        other_rows = [r for r in chunk if get_action(r) in ['update', 'delete']]

        # ======================
        # Process ADD rows (TWO-BATCH approach)
        # ======================
        if add_rows:
            # Check which ADD rows need permissions
            add_with_perms = []
            add_perm_data = []

            for row in add_rows:
                add_users = parse_user_ids(row.get("Add_UserIDs", ""))
                remove_users = parse_user_ids(row.get("Remove_UserIDs", ""))

                if add_users or remove_users:
                    add_with_perms.append(row)
                    add_perm_data.append({
                        'add_users': add_users,
                        'remove_users': remove_users
                    })

            print(f"📦 Batch {batch_number}: Creating {len(add_rows)} items ({len(add_with_perms)} need permissions)...")

            try:
                # BATCH 1: Create items
                batch_content, batch_id, processed = generate_create_batch(
                    add_rows, base_url, batch_number
                )

                if processed > 0:
                    total_processed += processed

                    debug_file = os.path.join(EXPORT_DIR, f"batch_{batch_number:04d}_create.txt")
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(batch_content)

                    print(f"   📄 Saved to {debug_file}")
                    print(f"   🚀 Sending CREATE batch...")

                    result = send_to_power_automate(
                        batch_body=batch_content,
                        target_url=base_url,
                        batch_boundary=batch_id,
                        pa_webhook_url=pa_webhook_url,
                        batch_num=batch_number
                    )

                    results.append(result)

                    if result.get("status") == "timeout":
                        print(f"   ⏱️  Batch {batch_number} timed out (likely successful, verify in PA)")
                        timeout_batches.append(batch_number)
                    elif result["success"]:
                        print(f"   ✅ Batch {batch_number} CREATE completed")

                        # Extract ItemIds from PA response
                        if add_with_perms:
                            item_ids = []

                            # Parse itemIds from PA response
                            if "itemIds" in result.get("response", {}):
                                item_ids_data = result["response"]["itemIds"]
                                item_ids = [item["Id"] for item in item_ids_data if "Id" in item]
                                print(f"   📋 Extracted {len(item_ids)} ItemIds from response")

                            if len(item_ids) == len(add_with_perms):
                                # BATCH 2: Set permissions automatically
                                batch_number += 1
                                print(f"\n📦 Batch {batch_number}: Setting permissions on {len(item_ids)} items...")

                                perm_batch_content, perm_batch_id, perm_processed = generate_permissions_batch(
                                    item_ids, add_perm_data, base_url, role_id, batch_number
                                )

                                perm_debug_file = os.path.join(EXPORT_DIR, f"batch_{batch_number:04d}_permissions.txt")
                                with open(perm_debug_file, "w", encoding="utf-8") as f:
                                    f.write(perm_batch_content)

                                print(f"   📄 Saved to {perm_debug_file}")
                                print(f"   🚀 Sending PERMISSIONS batch...")

                                perm_result = send_to_power_automate(
                                    batch_body=perm_batch_content,
                                    target_url=base_url,
                                    batch_boundary=perm_batch_id,
                                    pa_webhook_url=pa_webhook_url,
                                    batch_num=batch_number
                                )

                                results.append(perm_result)

                                if perm_result.get("status") == "timeout":
                                    print(f"   ⏱️  Permissions batch timed out")
                                    timeout_batches.append(batch_number)
                                elif perm_result["success"]:
                                    print(f"   ✅ Permissions set successfully")
                                else:
                                    print(f"   ❌ Permissions batch failed")
                                    failed_batches.append(batch_number)

                            elif len(item_ids) == 0:
                                print(f"   ⚠️  No ItemIds returned from PA, skipping permissions")
                            else:
                                print(f"   ⚠️  ItemId mismatch: Expected {len(add_with_perms)}, got {len(item_ids)}")
                                print(f"   ⚠️  Skipping permissions for this batch")

                        if delay_between_batches > 0:
                            print(f"   ⏳ Waiting {delay_between_batches}s...\n")
                            time.sleep(delay_between_batches)

                    else:
                        print(f"   ❌ CREATE batch failed")
                        failed_batches.append(batch_number)

            except Exception as e:
                print(f"   ❌ Exception in batch {batch_number}: {str(e)}")
                failed_batches.append(batch_number)
                results.append({
                    "success": False,
                    "batch_number": batch_number,
                    "error": str(e)
                })

            batch_number += 1

        # ======================
        # Process UPDATE/DELETE rows (SINGLE-BATCH with permissions)
        # ======================
        if other_rows:
            print(f"📦 Batch {batch_number}: Processing {len(other_rows)} UPDATE/DELETE rows...")

            try:
                batch_content, batch_id, processed = generate_batch_with_permissions(
                    other_rows, base_url, role_id, batch_number
                )

                if processed > 0:
                    total_processed += processed

                    debug_file = os.path.join(EXPORT_DIR, f"batch_{batch_number:04d}.txt")
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(batch_content)

                    print(f"   📄 Saved to {debug_file}")
                    print(f"   🚀 Sending to Power Automate...")

                    result = send_to_power_automate(
                        batch_body=batch_content,
                        target_url=base_url,
                        batch_boundary=batch_id,
                        pa_webhook_url=pa_webhook_url,
                        batch_num=batch_number
                    )

                    results.append(result)

                    if result.get("status") == "timeout":
                        print(f"   ⏱️  Batch {batch_number} timed out")
                        timeout_batches.append(batch_number)
                    elif result["success"]:
                        print(f"   ✅ Batch {batch_number} completed")
                    else:
                        print(f"   ❌ Batch {batch_number} failed")
                        failed_batches.append(batch_number)

                if delay_between_batches > 0:
                    print(f"   ⏳ Waiting {delay_between_batches}s...\n")
                    time.sleep(delay_between_batches)

            except Exception as e:
                print(f"   ❌ Exception in batch {batch_number}: {str(e)}")
                failed_batches.append(batch_number)
                results.append({
                    "success": False,
                    "batch_number": batch_number,
                    "error": str(e)
                })

            batch_number += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total Rows Read: {total_rows}")
    print(f"Total Rows Processed: {total_processed}")
    print(f"Total Batches: {batch_number - 1}")
    print(f"✅ Successful: {len([r for r in results if r.get('success') and r.get('status') != 'timeout'])}")
    print(f"⏱️  Timed Out: {len(timeout_batches)} (verify in PA)")
    print(f"❌ Failed: {len(failed_batches)}")

    if timeout_batches:
        print(f"\n⏱️  Timeout Batches: {timeout_batches}")

    if failed_batches:
        print(f"\n❌ Failed Batches: {failed_batches}")

    print(f"{'='*60}\n")

    return results


# -----------------------
# Main entrypoint
# -----------------------

def main(df, list_name: str):
    """
    Main entrypoint accepting a pandas DataFrame `df` and the SharePoint list name `list_name`.
    """
    # Configuration
    target_url = f"https://razrgroup-my.sharepoint.com/personal/communication_razor-group_com/_api/web/lists/GetByTitle('{list_name}')/items"

    # Power Automate webhook URL
    PA_WEBHOOK_URL = "https://default0922decaaf3c4870acea84b9557b04.6a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/b67ce8f71f8643ca9f389aedd590a7c4/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=gNckNGyNWSVsioZAjaGybgowiJ-shdo39WLth0lE6HA"

    # Permission Configuration
    ROLE_ID = 1073741827  # SharePoint Role ID (1073741826 = Read, 1073741827 = Edit, 1073741829 = Full Control)

    # Batch Configuration
    CHUNK_SIZE = 30
    DELAY_BETWEEN_BATCHES = 2
    TEST_MODE_MAX_BATCHES = None  # Set to a number for testing, None for all

    if TEST_MODE_MAX_BATCHES:
        print(f"⚠ TEST MODE: Processing only first {TEST_MODE_MAX_BATCHES} batches")
        print(f"   Set TEST_MODE_MAX_BATCHES = None to process all\n")

    results = process_df_with_permissions(
        df=df,
        base_url=target_url,
        role_id=ROLE_ID,
        pa_webhook_url=PA_WEBHOOK_URL,
        chunk_size=CHUNK_SIZE,
        delay_between_batches=DELAY_BETWEEN_BATCHES,
        max_batches=TEST_MODE_MAX_BATCHES
    )

    # Save results
    results_file = "batch_permissions_results.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"📄 Detailed results saved to {results_file}")


if __name__ == "__main__":
    print("This module exposes `main(df, list_name)`. Call it with a pandas DataFrame and the list name.")
    # Example local CSV test (uncomment to run):
    # import pandas as pd
    # df_test = pd.read_csv("./Export/processed.csv", encoding="utf-8-sig")
    # main(df_test, list_name="Quality Inspection")
