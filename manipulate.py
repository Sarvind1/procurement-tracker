import os
import pandas as pd
import time
from datetime import datetime

today = pd.to_datetime(datetime.today().date())

def check_partial_conditions(group):
    # cond1 = (group['hs | sign-off shipment booking im line'] == "Yes").any()
    prd_check = group['prd'].apply(lambda x: False if pd.isna(x) or x == "" else (-90 <= (x - today).days <= 90))
    cond2 = prd_check.any()
    if cond2:
        valid_prds = group.loc[group['prd'].notna(), 'prd']
        if not valid_prds.empty:
            prd_min = valid_prds.min()
            return pd.Series({'email scheduled for': prd_min - pd.Timedelta(days=10)})
    return pd.Series({'email scheduled for': pd.NaT})

def manipulate(df, universal_path):

    # universal_path = r"C:/Users/SupplierCommunicatio/OneDrive - Razor HQ GmbH & Co. KG/Razor - Chetan_Locale/Procurement Trackers"
    # universal_path = r"C:/Users/ChetanPaliwal/OneDrive - Razor HQ GmbH & Co. KG/Razor - Procurement Trackers"
    main_path = rf"{universal_path}/Compliance"
    vendor_info_path = rf"./Mappings/vendor_email_map.xlsx"
    sent_csv_path = rf"{main_path}/QI Email Scheduler/already_sent.csv"

    df['id'] = df['name'].str.split(' ', n=1).str[0]
    df['prd'] = pd.to_datetime(df['prd'], errors='coerce')
    df['quality control date'] = pd.to_datetime(df['quality control date'], errors='coerce')

    df['line id'] = (
        df['line id']
        .astype(str)
        .replace('', '0')
        .str.replace(r'\.0$', '', regex=True)
    )

    df['po_razin_id'] = (
        df['document number'].astype(str) + df['item'].astype(str) + df['line id'].astype(str)
    )
    df['subset'] = df['sub status'].apply(
        lambda x: 'otif' if x in ['17. QC Schedule Pending', '20a. QC Release Missing'] else ''
    )

    df_x = df.copy()
    
    partial_df = df_x.groupby("po_razin_id").apply(check_partial_conditions).reset_index()
    partial_df = partial_df.dropna(subset=['email scheduled for'])

    future_df = df_x[df_x['po_razin_id'].isin(partial_df['po_razin_id'])].drop_duplicates(subset='po_razin_id', keep='first')
    future_df = future_df.merge(partial_df, on='po_razin_id', how='left')

    vendor_df = pd.read_excel(vendor_info_path).drop_duplicates(subset='id', keep='first')

    future_df['id'] = pd.to_numeric(future_df['id'], errors='coerce').fillna(0).astype(int)
    vendor_df['id'] = pd.to_numeric(vendor_df['id'], errors='coerce').fillna(0).astype(int)

    merge_cols = ['id', 'name', 'email', 'cm email', 'sm email']
    vendor_merge = [c for c in merge_cols if c in vendor_df.columns]
    future_df = future_df.merge(vendor_df[vendor_merge], on='id', how='left')

    sent_df = pd.read_csv(sent_csv_path, dtype=str)

    sent_already = set(sent_df['po_razin_id'].astype(str))
    future_df['sent already?'] = future_df['po_razin_id'].astype(str).isin(sent_already).map({True: "Yes", False: "No"})

    future_df_out = future_df.rename(columns={
        'item': 'razin',
        'line id': 'line_id',
        'batch_id': 'batch_id',
        'associated brands': 'brand',
        'id': 'vendor id',
        'name_x': 'vendor name',
        'email': 'vendor email'
    })

    future_df_out['vendor name'] = future_df_out['vendor name'].str.split(' ', n=1).str[1]
    future_df_out['send early?'] = ""

    final_columns = [
        'po_razin_id', 'batch_id', 'document number', 'sent already?', 'email scheduled for', 'send early?',
        'prd', 'razin', 'line_id', 'brand', 'vendor id', 'vendor name', 'vendor email',
        'cm email', 'sm email', 'list_id', 'compliance status', 'quality control status', 'quality control date',
        "current status", "sub status", "days bucket", "final team", "final poc", "cm", "sm", "name",
        'subset'
    ]

    # static_names = list
    # dynamic_names = list

    # final_new_names = set (static_names + dynamic_names)

    # final_new_ids = id_extrater(final_new_names)

    # df2 = df2 + final_new_ids



    for col in final_columns:
        if col not in future_df_out.columns:
            future_df_out[col] = ""

    future_df_out = future_df_out[final_columns]

    future_df_out['quality control status'] = future_df_out['quality control status'].str.replace(r'^\d+\s*\|?\s*', '', regex=True)

    future_df_out['vendor id'] = (
        future_df_out['vendor id']
        .astype(str)
        .replace('', '0')
        .str.replace(r'\.0$', '', regex=True)
    )

    future_df_out['key'] = future_df_out['po_razin_id'].astype(str) + future_df_out['vendor id']

    default_date = pd.to_datetime("1999-01-01")

    # future_df_out['email scheduled for'] = pd.to_datetime(
    #     future_df_out['email scheduled for'], errors='coerce'
    # ).fillna(default_date).dt.strftime('%Y-%m-%d')

    # future_df_out['prd'] = pd.to_datetime(
    #     future_df_out['prd'], errors='coerce'
    # ).fillna(default_date).dt.strftime('%Y-%m-%d')

    # future_df_out['quality control date'] = pd.to_datetime(
    #     future_df_out['quality control date'], errors='coerce'
    # ).fillna(default_date).dt.strftime('%Y-%m-%d')

    future_df_out['email scheduled for'] = pd.to_datetime(
        future_df_out['email scheduled for'], errors='coerce'
    ).dt.strftime('%Y-%m-%d')

    future_df_out['prd'] = pd.to_datetime(
        future_df_out['prd'], errors='coerce'
    ).dt.strftime('%Y-%m-%d')

    future_df_out['quality control date'] = pd.to_datetime(
        future_df_out['quality control date'], errors='coerce'
    ).dt.strftime('%Y-%m-%d')

    future_df_out['vendor id'] = (
        future_df_out['vendor id']
        .replace('', pd.NA)
        .fillna(0)
    )

    # future_df_out= future_df_out[future_df_out['po_razin_id']=='PO378536EVER-0000874']

    os.makedirs("Export", exist_ok=True)

    file_path = os.path.join("Export", "processed_df2.csv")

    future_df_out.to_csv(file_path, index=False, encoding='utf-8')

    return future_df_out