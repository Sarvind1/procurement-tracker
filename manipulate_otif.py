import os
import pandas as pd
import time
from datetime import datetime

today = pd.to_datetime(datetime.today().date())


def manipulate(otif, otif_tracker, rgbit_netsuite, shipment_telex):

    # otif = otif[otif['sm']!="Boris Denais"]

    trackers = [
        {'process': 'Supp Confirm', 'filter': [{'column name': 'current status', 'criteria': ['02. Supplier Confirmation Pending']}]},
        {'process': 'Prod Delayed', 'filter': [{'column name': 'sub status', 'criteria': ['09a. Production Delayed']}]},
        {'process': 'PRD', 'filter': [{'column name': 'current status', 'criteria': ['06. Packaging Pending', '07. Transperancy Label Pending', '08. PRD Pending']}, {'column name': 'prd', 'criteria': [None]}]},
        {'process': 'G2', 'filter': [{'column name': 'current status', 'criteria': ['11. PO Line Sign-Off Pending', '12. Ready for Batching Pending', '13. Batch Creation Pending']}]},
        # {'process': 'CPRD', 'filter': [{'column name': 'current status', 'criteria': ['10. PRD Confirmation Pending']}, {'column name': 'prd reconfirmed', 'criteria': ['No']}]},
        {'process': 'CPRD', 'filter': [{'column name': 'current status', 'criteria': ['10. PRD Confirmation Pending']}]},
        {'process': 'PP', 'filter': [{'column name': 'sub status', 'criteria': ['03. PI Upload Pending', '04a. SM Review Pending', '04b. Accounting Approval Pending', '05c. Rejected']}]},
        {'process': 'TLX', 'filter': [{'column name': 'sub status', 'criteria': ['26a. Approved','26b. Pending Approval','26c. Rejected','27a. Approved','27b. Pending Approval','27c. Rejected','28a. In Transit - Supplier Pending','28b. In Transit - SM Pending','28c. In Transit - FFW Pending','28d. Arrived - Supplier Pending','28e. Arrived - SM Pending','28f. Arrived - FFW Pending','28g. Released - Not Arrived']}]},
        {'process': 'Pickup', 'filter': [{'column name': 'sub status', 'criteria': ['20a. QC Release Missing','20e. FOB Date Missing', '21a. YW1 - Pickup Pending', '21b. Local - Delivery Pending', '21c. YW1 - Receiving Pending', '21e. DDP/DAP/FOB Date in Past','21f. FOB Date in <2 Days','21g. FOB Date in Future','22a. No Dependency','22b. Procurement Dependency','22c. FFW Dependency']}]},
        {'process': 'BP', 'filter': [{'column name': 'current status', 'criteria': ['15. CI Approval Pending', '16. CI Payment Pending', '17. QC Schedule Pending', '18. Freight Booking Missing', '19. Supplier Pickup Date Pending', '20. Pre Pickup Check', '22. Freight Pickup Pending', '23. INB Creation Pending', '24. Mark In-Transit Pending', '25. BL Approval Pending', '26. BL Payment Pending - In Transit', '27. BL Payment Pending - Arrived', '28. Telex Release Pending', '29. Custom Clearance Pending', '30. Stock Delivery Pending', '31. Stock Receiving Pending', '32. Dispute - PO Closing Pending']}, {'column name': 'invoice number', 'criteria': [None]}]},
        {'process': 'Customs', 'filter': [{'column name': 'current status', 'criteria': ['26. BL Payment Pending - In Transit','27. BL Payment Pending - Arrived','28. Telex Release Pending','29. Custom Clearance Pending']}]},
        {'process': 'Compliance', 'filter': [{'column name': 'current status', 'criteria': ['B. Compliance Blocked','03. PI Upload Pending','04. PI Approval Pending','05. PI Payment Pending','06. Packaging Pending','07. Transperancy Label Pending','08. PRD Pending','09. Under Production','10. PRD Confirmation Pending','11. IM Sign-Off Pending']}, {'column name': 'compliance status', 'criteria': ['Blocked','To Be Tested','Missing','To Be Reviewed',' ',0]}]},
        {'process': 'SPD', 'filter': [{'column name': 'sub status', 'criteria': ['17. QC Schedule Pending','19a. Booking Form Not Sent','19b. SPD Missing','19c. Future SPD - 7+ Days','19d. SPD Blocked']}]},
        {'process': 'G4', 'filter': [{'column name': 'sub status', 'criteria': ['14a. AVA Email Not Sent','14b. Documents Missing','14c. SM Sign-Off Missing']}]},
        {'process': 'QC', 'filter': [{'column name': 'sub status', 'criteria': ['17. QC Schedule Pending', '20a. QC Release Missing']}]}
    ]

    def matches_tracker(row, tracker):
        for f in tracker.get('filter', []):
            col = f['column name']
            if col not in row.index:
                return False
            
            val = row[col]
            criteria = f['criteria']
            
            if criteria == ['Not None']:
                if pd.isna(val) or str(val).strip() == '':
                    return False
            else:
                if pd.isna(val):
                    val_str = None
                else:
                    val_str = str(val).strip()
                criteria_str = [str(c).strip() if c is not None else None for c in criteria]
                if val_str not in criteria_str:
                    return False
        return True

    otif['tracker'] = ''

    for tracker in trackers:
        mask = otif.apply(lambda row: matches_tracker(row, tracker), axis=1)
        
        otif.loc[mask, 'tracker'] = otif.loc[mask, 'tracker'].apply(
            lambda x: f"{x}, {tracker['process']}" if x else tracker['process']
        )

    def reorder_tracker(val):
        if not val:
            return val
        parts = [p.strip() for p in val.split(",") if p.strip()]
        if "BP" in parts and len(parts) > 1:
            parts.remove("BP")
            parts.append("BP")
        return ", ".join(parts)

    otif['tracker'] = otif['tracker'].apply(reorder_tracker)
    ## end ##

    otif = otif[otif['tracker'].notna() & (otif['tracker'] != "")]

    otif['SM G2 Status'] = ""
    otif['SM G2 Blocker'] = ""
    otif['SM G2 Comments'] = ""
    otif['SM G4 Status'] = ""
    otif['SM G4 Blocker'] = ""
    otif['SM G4 Comments'] = ""
    otif['SM Pickup Status'] = ""
    otif['SM Pickup Blocker'] = ""
    otif['SM Pickup Comments'] = ""
    otif['SM SPD Status'] = ""
    otif['SM SPD Blocker'] = ""
    otif['SM SPD Comments'] = ""
    otif['SM TLX Status'] = ""
    otif['SM TLX Blocker'] = ""
    otif['SM TLX Comments'] = ""
    otif['SM BP Status'] = ""
    otif['SM BP Blocker'] = ""
    otif['SM BP Comments'] = ""
    otif['SM PP Status'] = ""
    otif['SM PP Blocker'] = ""
    otif['SM PP Comments'] = ""
    otif['SM CPRD Status'] = ""
    otif['SM CPRD Blocker'] = ""
    otif['SM CPRD Comments'] = ""
    otif['SM PRD Status'] = ""
    otif['SM PRD Blocker'] = ""
    otif['SM PRD Comments'] = ""

    otif['update_type'] = ""
    otif['initial_access'] = ""
    otif['final_access'] = ""
    otif['final_access_email'] = ""
    otif['poc_changed'] = ""
    otif['quantity'] = otif['quantity'].replace(['', ' '], 0).fillna(0)
    otif['quantity fulfilled/received'] = otif['quantity fulfilled/received'].replace(['', ' '], 0).fillna(0)
    otif['quantity on shipments'] = otif['quantity on shipments'].replace(['', ' '], 0).fillna(0)

    otif['deliver to location'] = otif['po_razin_id'].map(rgbit_netsuite.drop_duplicates(subset='otif_id', keep='first').set_index('otif_id')['deliver to location'])
    # otif['deliver to location'] = ""

    otif['container number'] = otif['inb#'].map(shipment_telex.set_index('inb#')['container number'])
    otif['actual arrival date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['actual arrival date'])
    otif['actual shipping date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['actual shipping date'])
    otif['actual pickup date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['actual pickup date'])
    otif['gate in'] = otif['inb#'].map(shipment_telex.set_index('inb#')['gate in'])
    otif['expected shipping date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['expected shipping date'])
    otif['actual delivery date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['actual delivery date'])
    otif['ffw'] = otif['inb#'].map(shipment_telex.drop_duplicates(subset='inb#', keep='first').set_index('inb#')['ffw'])
    otif['telex release date (supplier)'] = otif['inb#'].map(shipment_telex.set_index('inb#')['telex release date (supplier)'])
    otif['telex release date (ffwp)'] = otif['inb#'].map(shipment_telex.set_index('inb#')['telex release date (ffwp)'])
    otif['expected arrival date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['expected arrival date'])
    otif['expected delivery date'] = otif['inb#'].map(shipment_telex.set_index('inb#')['expected delivery date'])

    otif['spd'] = otif['batch_id'].map(otif_tracker.set_index('batch_id')['spd'])

    otif = otif.rename(columns={"hs | sign-off shipment booking im line": "im sign-off", "hs | sign-off shipment booking sm line": "sm sign-off"})

    otif = otif.rename(columns={'po_razin_id':'otif_id'})

    otif['po_razin_id'] = otif['otif_id']
    otif['id'] = otif['name'].str.split(" ").str[0]
    otif['po_razin_vendor'] = otif['document number'].astype(str)+ otif['item'].astype(str) + otif['id'].astype(str)

    otif = otif[[
        'otif_id', 'po_razin_id',
        'SM G2 Status', 'SM G2 Blocker', 'SM G2 Comments', 'SM G4 Status', 'SM G4 Blocker','SM G4 Comments',
        'SM Pickup Status', 'SM Pickup Blocker', 'SM Pickup Comments', 'SM SPD Status', 'SM SPD Blocker','SM SPD Comments',
        'SM TLX Status', 'SM TLX Blocker', 'SM TLX Comments',
        'SM BP Status', 'SM BP Blocker', 'SM BP Comments', 'SM PP Status', 'SM PP Blocker', 'SM PP Comments',
        'SM CPRD Status', 'SM CPRD Blocker', 'SM CPRD Comments', 'SM PRD Status', 'SM PRD Blocker', 'SM PRD Comments',
        'current status','sub status','days bucket','final team','final poc',
        'date created','document number','item','line id','asin number','market place','associated brands','incoterm',
        'name','supplier payment terms','supplier confirmation status','memo (main)','first prd','prd','planned prd','confirmed crd',
        'quantity','quantity fulfilled/received','quantity on shipments','im sign-off','sm sign-off',
        'production status','batch_id','wh type','prd reconfirmed','invoice number','invoice status','per unit amount','pending units','pending value',
        'line payment type','batch payment type','inb payment type','line invoice submission status','batch invoice submission status',
        'inb invoice submission status','line payment status','batch payment status','inb payment status','batch qc pending','vp booking status',
        'fob date','fob status','batch pickup status','shipping status','inb#','shipment_status','shipment_substatus','supplier telex status',
        'sm telex status','ffw telex status','compliance status','quality control date','quality control status','cm','sm','team','l2 final status',
        'deliver to location', 'internal id', 'po_razin_vendor',
        'container number', 'actual arrival date', 'actual shipping date', 'gate in', 'expected shipping date', 'actual delivery date', 'ffw',
        'telex release date (supplier)', 'telex release date (ffwp)', 'expected arrival date', 'expected delivery date',
        'spd',
        'update_type', 'initial_access', 'final_access', 'final_access_email', 'poc_changed', 'tracker'
    ]]

    ## grouping starts ##
    otif["mask_flag"] = otif["tracker"].str.contains("G4|TLX|BP|SPD", na=False)

    otif["mask_pp_flag"] = otif["tracker"].str.contains("PP", na=False)

    otif['po_razin_id_count'] = otif.groupby('document number')['po_razin_id'].transform('count')

    batch_docs = (
        otif[otif["mask_flag"]]
        .groupby("batch_id")
        .agg({
            "document number": lambda x: ",".join(sorted(set(map(str, x)))),
            "po_razin_id_count": "max"
        })
        .reset_index()
    )

    batch_docs["id_col"] = (
        batch_docs["batch_id"].astype(str) + " - " +
        batch_docs["document number"] + " - " +
        batch_docs["po_razin_id_count"].astype(str)
    )

    po_docs = (
        otif[otif["mask_pp_flag"]]
        .groupby("document number")
        .agg({
            "po_razin_id_count": "max"
        })
        .reset_index()
    )

    po_docs["id_col"] = (
        po_docs["document number"] + " - " +
        po_docs["po_razin_id_count"].astype(str)
    )

    otif = otif.drop(columns=["id_col"], errors="ignore")

    otif = otif.merge(batch_docs[["batch_id", "id_col"]], on="batch_id", how="left")

    otif = otif.merge(po_docs[["document number", "id_col"]], 
                    on="document number", how="left", suffixes=("", "_po"))

    otif["id_col"] = otif.apply(
        lambda x: (
            x["id_col"]
            if pd.notna(x["id_col"]) else (
                x["id_col_po"]
                if pd.notna(x["id_col_po"]) else str(x["po_razin_id"])
            )
        ),
        axis=1
    )

    otif = otif.drop(columns=["id_col_po"], errors="ignore")

    sum_columns = [
        'quantity', 'quantity fulfilled/received', 'quantity on shipments',
        'pending units', 'pending value', 'per unit amount', 'internal id', 'line id'
    ]

    agg_dict = {
        "tracker": "first",
        "id_col": "first",
        "item": lambda x: ",".join(map(str, x)),
        # "line id": lambda x: ",".join(map(str, x)),
        "asin number": lambda x: ",".join(map(str, x)),
        "market place": lambda x: ",".join(map(str, x))
    }

    for col in otif.columns:
        if col not in agg_dict and col != "batch_id":
            if col in sum_columns:
                agg_dict[col] = "sum"
            else:
                agg_dict[col] = lambda x: ",".join(sorted(set(map(str, x))))

    # ------------------------
    # Collapse mask_flag (batch_id)
    # ------------------------
    collapsed_batch = (
        otif[otif["mask_flag"]]
        .groupby("batch_id", as_index=False, sort=False)
        .agg(agg_dict)
        .drop(columns=["otif_id"], errors="ignore")
        .rename(columns={'id_col': 'otif_id'})
    )

    # ------------------------
    # Collapse mask_pp_flag (document number)
    # ------------------------
    collapsed_pp = (
        otif[otif["mask_pp_flag"]]
        .groupby("document number", as_index=False, sort=False)
        .agg(agg_dict)
        .drop(columns=["otif_id"], errors="ignore")
        .rename(columns={'id_col': 'otif_id'})
    )

    # ------------------------
    # Remaining rows (neither flag)
    # ------------------------
    otif_filtered = otif[~(otif["mask_flag"] | otif["mask_pp_flag"])].reset_index(drop=True)

    # ------------------------
    # Final combined result
    # ------------------------
    result_df = pd.concat([otif_filtered, collapsed_batch, collapsed_pp], ignore_index=True)

    # Column selection
    result_df = result_df[[ 
        'otif_id', 'po_razin_id',
        'SM G2 Status','SM G2 Blocker','SM G2 Comments','SM G4 Status','SM G4 Blocker','SM G4 Comments',
        'SM Pickup Status','SM Pickup Blocker','SM Pickup Comments','SM SPD Status','SM SPD Blocker','SM SPD Comments',
        'SM TLX Status','SM TLX Blocker','SM TLX Comments',
        'SM BP Status','SM BP Blocker','SM BP Comments','SM PP Status','SM PP Blocker','SM PP Comments',
        'SM CPRD Status','SM CPRD Blocker','SM CPRD Comments','SM PRD Status','SM PRD Blocker','SM PRD Comments',
        'current status','sub status','days bucket','final team','final poc',
        'date created','document number','item','line id','asin number','market place','associated brands','incoterm',
        'name','supplier payment terms','supplier confirmation status','memo (main)','first prd','prd','planned prd','confirmed crd',
        'quantity','quantity fulfilled/received','quantity on shipments','im sign-off','sm sign-off',
        'production status','batch_id','wh type','prd reconfirmed','invoice number','invoice status','per unit amount','pending units','pending value',
        'line payment type','batch payment type','inb payment type','line invoice submission status','batch invoice submission status',
        'inb invoice submission status','line payment status','batch payment status','inb payment status','batch qc pending','vp booking status',
        'fob date','fob status','batch pickup status','shipping status','inb#','shipment_status','shipment_substatus','supplier telex status',
        'sm telex status','ffw telex status','compliance status','quality control date','quality control status','cm','sm','team','l2 final status',
        'deliver to location','internal id', 'po_razin_vendor',
        'container number','actual arrival date','actual shipping date','gate in','expected shipping date','actual delivery date','ffw',
        'telex release date (supplier)','telex release date (ffwp)','expected arrival date','expected delivery date',
        'spd',
        'update_type','initial_access','final_access','final_access_email','poc_changed','tracker'
    ]]
    
    # Clean po_razin_id
    result_df['po_razin_id'] = result_df['po_razin_id'].str.split(",", n=1).str[0]

    ## grouping ends ##

    otif = result_df.copy()

    os.makedirs("Export", exist_ok=True)

    file_path = os.path.join("Export", "processed_df2_otif.csv")

    otif.to_csv(file_path, index=False, encoding='utf-8')

    return otif