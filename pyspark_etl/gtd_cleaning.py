import pandas as pd
import numpy as np
import os

# === Load data ===
file_path = r"C:\Users\james\OneDrive\Desktop\Tableau_UW\globalterrorismdb_cleaned.csv"
df = pd.read_csv(file_path, encoding='latin1')

# === Track initial row count ===
initial_rows = len(df)
print(f"📊 Initial rows: {initial_rows}")

# === Text normalization ===
placeholder_vals = ['unknown', 'not applicable', 'unk', 'none', 'nan', '']
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].astype(str).str.lower().str.strip()
    df[col] = df[col].replace(placeholder_vals, np.nan)

df.replace(['<na>', 'nan'], np.nan, inplace=True)

# === Clean binary/boolean fields ===
binary_cols = ['claimed', 'ishostkid', 'INT_ANY', 'INT_MISC', 'INT_IDEO', 'INT_LOG',
               'property', 'doubtterr', 'vicinity']
for col in binary_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# === Drop rows missing lat/lon ===
missing_latlon = df[df['latitude'].isnull() | df['longitude'].isnull()]
print(f"🗺️ Rows dropped due to missing lat/lon: {len(missing_latlon)}")
df.dropna(subset=['latitude', 'longitude'], inplace=True)

# === Fill common missing fields ===
df['city'] = df['city'].fillna('unknown')
df['provstate'] = df['provstate'].fillna('unknown')

# === Code–Text alignment check ===
def check_alignment(df, code_col, text_col):
    mismatches = df.groupby(code_col)[text_col].nunique()
    if mismatches.max() > 1:
        print(f"\n🚨 MISMATCH in {code_col} ↔ {text_col}:")
        print(mismatches[mismatches > 1])

alignment_pairs = [
    ('country', 'country_txt'),
    ('region', 'region_txt'),
    ('attacktype1', 'attacktype1_txt'),
    ('targtype1', 'targtype1_txt'),
    ('targsubtype1', 'targsubtype1_txt'),
    ('weaptype1', 'weaptype1_txt'),
    ('weapsubtype1', 'weapsubtype1_txt')
]
for code_col, text_col in alignment_pairs:
    if code_col in df.columns and text_col in df.columns:
        check_alignment(df, code_col, text_col)

# === Type integrity check and export of bad rows ===
expected_numeric_cols = [
    'iyear', 'imonth', 'iday', 'nkill', 'nkillus', 'nkillter',
    'nwound', 'nwoundus', 'nwoundte', 'propvalue', 'nperps',
    'nperpcap', 'ransomamt', 'ransomamtus', 'ransompaid', 'ransompaidus'
]

export_dir = r"C:\Users\james\OneDrive\Desktop\Tableau_UW\bad_numeric_rows"
os.makedirs(export_dir, exist_ok=True)

for col in expected_numeric_cols:
    if col in df.columns:
        invalid_mask = pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()
        bad_rows = df[invalid_mask]
        if not bad_rows.empty:
            print(f"\n🚨 Non-numeric entries found in '{col}' (exported):")
            print(bad_rows[[col]].drop_duplicates().head(10))
            export_path = os.path.join(export_dir, f"bad_{col}.csv")
            bad_rows.to_csv(export_path, index=False)
            print(f"🧾 Saved: {export_path}")

        # Safe type conversion
        df[col] = pd.to_numeric(df[col], errors='coerce')

# === Rename key columns ===
rename_map = {
    "iyear": "year",
    "imonth": "month",
    "iday": "day",
    "nkill": "num_killed",
    "nwound": "num_wounded",
    "nkillus": "num_us_killed",
    "nwoundus": "num_us_wounded",
    "nkillter": "num_terrorists_killed",
    "nwoundte": "num_terrorists_wounded"
}
df.rename(columns=rename_map, inplace=True)

# === Drop specific unwanted columns ===
df = df.drop([
    "summary", "motive", "addnotes", "propcomment", "weapdetail",
    "target1", "target2", "target3", "corp1", "corp2", "corp3",
    "claimmode_txt", "claimmode2", "claimmode3",
    "scite1", "scite2", "scite3", "approxdate", "resolution", "ransomnote",
    "gsubname", "gsubname2", "gsubname3", "related", "location",
    "vicinity", "specificity",
    "dbsource", "INT_LOG", "INT_IDEO", "INT_MISC", "INT_ANY",
    "attacktype2", "attacktype2_txt", "attacktype3", "attacktype3_txt"
], axis=1, errors='ignore')

# === Drop columns with ≥80% null values ===
sparse_cols = df.columns[df.isnull().mean() >= 0.8].tolist()
print(f"\n🧹 Dropping sparse columns (≥80% null): {sparse_cols}")
df.drop(columns=sparse_cols, inplace=True)

# === Final row count ===
final_rows = len(df)
print(f"\n✅ Final rows: {final_rows}")
print(f"❌ Total rows dropped: {initial_rows - final_rows}")

# === Export cleaned file ===
output_path = r"C:\Users\james\OneDrive\Desktop\Tableau_UW\globalterrorismdb_integrity_cleaned.csv"
df.to_csv(output_path, index=False)
print(f"📁 Cleaned dataset saved to: {output_path}")
