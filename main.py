from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from datetime import datetime

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process-csv")
async def process_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    try:
        # ---------- Save uploaded file ----------
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())

        # ---------- Read CSV ----------
        df = pd.read_csv(file_path)

        if df.empty:
            raise HTTPException(status_code=400, detail="CSV is empty")

        # ---------- Select numeric columns ----------
        X = df.select_dtypes(include=["number"])

        if X.shape[1] < 2:
            raise HTTPException(
                status_code=400,
                detail="Need at least 2 numeric columns for PCA"
            )

        # ---------- Impute NaN values ----------
        imputer = SimpleImputer(strategy="mean")
        X_imputed = imputer.fit_transform(X)

        # ---------- Scale data ----------
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_imputed)

        # ---------- PCA ----------
        pca = PCA(n_components=2)
        pca_components = pca.fit_transform(X_scaled)

        # ---------- Add PCA columns ----------
        df["PCA_1"] = pca_components[:, 0]
        df["PCA_2"] = pca_components[:, 1]

        # ---------- Save new CSV ----------
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"processed_{timestamp}.csv"
        output_path = os.path.join(UPLOAD_DIR, output_filename)

        df.to_csv(output_path, index=False)

        return {
            "message": "CSV processed successfully",
            "rows": len(df),
            "columns": list(df.columns),
            "saved_file": output_filename,
            "explained_variance": pca.explained_variance_ratio_.tolist(),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    