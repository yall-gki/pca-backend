import os
import csv
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="CSV Processor")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/process-csv")
async def process_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    try:
        rows = []
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert empty strings to None
                row = {k: (v if v.strip() != "" else None) for k, v in row.items()}
                rows.append(row)

        total_rows = len(rows)
        columns = list(rows[0].keys()) if rows else []
        seen = set()
        unique_rows = []
        duplicate_rows_idx = []

        for i, row in enumerate(rows):
            row_tuple = tuple((k, str(v)) for k, v in row.items())
            if row_tuple in seen:
                duplicate_rows_idx.append(i)
            else:
                seen.add(row_tuple)
                unique_rows.append(row)

        # Count duplicates per column
        col_duplicates = {}
        for col in columns:
            col_values = [row[col] for row in rows]
            col_duplicates[col] = len(col_values) - len(set(col_values))

        return {
            "columns": columns,
            "total_rows": total_rows,
            "duplicate_rows_count": len(duplicate_rows_idx),
            "duplicate_rows_indices": duplicate_rows_idx,
            "duplicates_per_column": col_duplicates,
            "unique_rows_count": len(unique_rows),
            "unique_data": unique_rows,
        }

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

