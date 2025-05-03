import os
import pandas as pd
import sqlite3

# Path to folder with Excel files
folder_path = "files"

# Gather all Excel files
excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]

all_sheets = {}  # sheet_name -> list of DataFrames
next_post_id = 1
next_comment_id = 1

# Process each file
for file_index, file_name in enumerate(excel_files):
    file_path = os.path.join(folder_path, file_name)
    sheets = pd.read_excel(file_path, sheet_name=None)

    file_post_id_mapping = {}

    # Step 1: Build post-id mapping
    for sheet_name, sheet in sheets.items():
        if sheet_name.lower() == 'events':
            continue
        if 'post-id' in sheet.columns:
            for old_id in sheet['post-id']:
                if pd.notnull(old_id):
                    try:
                        old_id = int(old_id)
                        if old_id not in file_post_id_mapping:
                            file_post_id_mapping[old_id] = next_post_id
                            next_post_id += 1
                    except:
                        continue

    # Step 2: Apply mappings and label/comment_id modifications
    for sheet_name, sheet in sheets.items():
        sheet = sheet.copy()
        sheet_name_clean = sheet_name.strip().lower()

        # Map post-id
        if sheet_name_clean != 'events':
            for col in sheet.columns:
                if sheet[col].dtype == 'object' or pd.api.types.is_numeric_dtype(sheet[col]):
                    sheet[col] = sheet[col].apply(
                        lambda x: file_post_id_mapping.get(int(x), x)
                        if pd.notnull(x) and str(x).isdigit() and int(x) in file_post_id_mapping
                        else x
                    )

        # Add label columns
        if sheet_name_clean == 'post features':
            for col in ['annotatorOne_post_label', 'annotatorTwo_post_label', 'annotatorThree_post_label']:
                if col not in sheet.columns:
                    sheet[col] = None

        elif sheet_name_clean == 'comments':
            # Assign comment_id starting from 1 and increasing
            sheet['comment_id'] = list(range(next_comment_id, next_comment_id + len(sheet)))
            next_comment_id += len(sheet)

            for col in ['annotatorOne_comment_label', 'annotatorTwo_comment_label', 'annotatorThree_comment_label', 'label']:
                if col not in sheet.columns:
                    sheet[col] = None

        # Skip 'events' sheet for all files except the first
        if sheet_name_clean == 'events' and file_index > 0:
            continue

        # Store updated sheet
        if sheet_name not in all_sheets:
            all_sheets[sheet_name] = []
        all_sheets[sheet_name].append(sheet)

# Step 3: Merge sheets
merged_sheets = {
    name: pd.concat(dfs, ignore_index=True) for name, dfs in all_sheets.items()
}

# Step 4: Save to SQLite
conn = sqlite3.connect("merged_social_data.db")
cursor = conn.cursor()

for sheet_name, df in merged_sheets.items():
    table_name = sheet_name.strip().replace(" ", "_").replace("-", "_")
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    clean_df = df.copy()
    for col in clean_df.columns:
        if clean_df[col].apply(lambda x: isinstance(x, (list, dict, set))).any():
            clean_df[col] = clean_df[col].apply(str)
        elif clean_df[col].dtype == 'object':
            clean_df[col] = clean_df[col].astype(str)

    clean_df = clean_df.where(pd.notnull(clean_df), None)
    clean_df.to_sql(table_name, conn, index=False)

conn.commit()
conn.close()

# Step 5: Save to Excel
with pd.ExcelWriter("merged_social_data.xlsx") as writer:
    for sheet_name, df in merged_sheets.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("✅ Merging complete!")
print("➡️ Total files processed:", len(excel_files))
print("➡️ Merged SQLite database: merged_social_data.db")
print("➡️ Merged Excel file: merged_social_data.xlsx")
