from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
import os
import tempfile
import traceback
from werkzeug.utils import secure_filename

app = Flask(__name__)

# ======================
# CORS CONFIG
# ======================
CORS(
    app,
    origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://project2frontend-theta.vercel.app"
    ],
    supports_credentials=True
)

UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {"xlsx"}

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ======================
# CORE PROCESSING LOGIC
# ======================
def process_excel_file(xlsx_path):
    try:
        # Read Excel safely (handles blank rows at top)
        df = pd.read_excel(xlsx_path)
        df = df.dropna(how="all")   # THIS handles blank rows safely


        # Drop fully empty rows
        df = df.dropna(how="all")

        # 🔥 CRITICAL FIX: normalize column names
        # Handles: "Sold\nQuantity", "Sold Quantity", extra spaces, tabs
        df.columns = (
            df.columns
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        # Columns to remove (safe even if missing)
        delete_column_names = [
            "Exch", "Book Type", "Settlement", "Transaction Date",
            "Order #", "Order Time", "Trade #", "Trade Time",
            "Terminal #", "CTCL Terminal #", "Txn Type",
            "Scrip Code", "*", "Expiry Date", "Strike Price",
            "O.T.", "Market Rate", "Bought Branch Code",
            "Bought Rate", "Sold Branch Code", "Sold Rate",
            "Brok-Cont", "Value-Brok"
        ]

        df = df.drop(
            columns=[c for c in delete_column_names if c in df.columns],
            errors="ignore"
        )

        # Ensure numeric conversion only if column exists
        for col in ["Bought Quantity", "Sold Quantity", "Mkt. Value"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # SYS18 / SYS27 cleanup (DO NOT touch quantities or values)
        for side in ["Bought", "Sold"]:
            code_col = f"{side} Code"
            name_col = f"{side} Name"

            if code_col in df.columns:
                mask = df[code_col].isin(["SYS18", "SYS27"])
                df.loc[mask, code_col] = None
                if name_col in df.columns:
                    df.loc[mask, name_col] = None

        # Guarantee quantity columns exist
        if "Bought Quantity" not in df.columns:
            df["Bought Quantity"] = np.nan
        if "Sold Quantity" not in df.columns:
            df["Sold Quantity"] = np.nan

        # Sold quantities must be negative
        df["Sold Quantity"] = df["Sold Quantity"].apply(
            lambda x: -abs(x) if pd.notnull(x) else x
        )

        # Ensure Market Value exists
        if "Mkt. Value" not in df.columns:
            df["Mkt. Value"] = 0

        # Market value sign follows sold quantity
        df["Mkt. Value"] = np.where(
            df["Sold Quantity"].notna(),
            -abs(df["Mkt. Value"]),
            df["Mkt. Value"]
        )

        # Merge bought & sold legs safely
        df["Final Code"] = df.get("Bought Code").combine_first(df.get("Sold Code"))
        df["Final Name"] = df.get("Bought Name").combine_first(df.get("Sold Name"))
        df["Final Quantity"] = df["Bought Quantity"].combine_first(df["Sold Quantity"])

        # Aggregate (IMPORTANT: dropna=False)
        summary = (
            df.groupby(
                ["Final Name", "Scrip Name", "Final Code"],
                dropna=False
            )
            .agg({
                "Final Quantity": "sum",
                "Mkt. Value": "sum"
            })
            .reset_index()
        )

        # Rename columns to required output format
        summary.columns = [
            "Bought Name",
            "Scrip Name",
            "Bought Code",
            "Sum of Bought Quantity",
            "Sum of Value"
        ]

        # Filter large transactions (matches your screenshot)
        summary = summary[
            (summary["Sum of Bought Quantity"].abs() >= 10_000) |
            (summary["Sum of Value"].abs() >= 1_000_000)
        ]

        return summary

    except Exception as e:
        raise Exception(f"Error processing file: {str(e)}")


# ======================
# ROUTES
# ======================
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "backend": "Flask",
        "version": "3.0.0"
    })


@app.route("/api/upload", methods=["POST"])
def upload_file():
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "Empty filename"}), 400

        if not allowed_file(file.filename):
            return jsonify({"error": "Only .xlsx files are allowed"}), 400

        filename = secure_filename(file.filename)
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, filename)

        file.save(file_path)

        result_df = process_excel_file(file_path)

        output_filename = f"processed_{filename.rsplit('.', 1)[0]}.csv"
        output_path = os.path.join(temp_dir, output_filename)

        result_df.to_csv(output_path, index=False)

        return jsonify({
            "success": True,
            "message": "File processed successfully",
            "total_records": len(result_df),
            "columns": list(result_df.columns),
            "preview": result_df.head(5).fillna("").to_dict("records"),
            "download_url": f"/api/download/{output_filename}"
        })

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/download/<filename>", methods=["GET"])
def download_file(filename):
    try:
        filename = secure_filename(filename)
        temp_dir = tempfile.gettempdir()

        for root, _, files in os.walk(temp_dir):
            if filename in files:
                return send_file(
                    os.path.join(root, filename),
                    as_attachment=True,
                    download_name=filename,
                    mimetype="text/csv"
                )

        return jsonify({"error": "File not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def create_app():
    return app


if __name__ == "__main__":
    app.run(debug=True, port=5002)
