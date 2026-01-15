from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
import os
import tempfile
import traceback
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configure CORS to allow requests from your React app
CORS(app, origins=["http://localhost:5173", "http://127.0.0.1:5173", "https://project2frontend-theta.vercel.app"], supports_credentials=True)

# Configuration
UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['CORS_HEADERS'] = 'Content-Type'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_excel_file(xlsx_path):
    """Process the Excel file using your provided logic"""
    try:
        # Read the Excel file
        df = pd.read_excel(xlsx_path)
        
        # Remove rows where all columns except the first are NaN
        df = df.dropna(subset=df.columns[1:], how='all')
        
        # Set new header from row 4
        new_header = df.iloc[0]
        df = df.iloc[5:].copy()
        df.columns = new_header
        df.reset_index(drop=True, inplace=True)
        
        # Delete unnecessary columns
        delete_column_names = ['Exch', 'Book Type', 'Settlement', 'Transaction Date', 'Order #', 
                              'Order Time', 'Trade #', 'Trade Time', 'Terminal #', 'CTCL Terminal #', 
                              'Txn Type', 'Scrip Code', '*', 'Expiry Date', 'Strike Price', 
                              'O.T.', 'Market Rate', 'Bought Branch Code', 'Bought Rate', 
                              'Sold Branch Code', 'Sold Rate', 'Brok-Cont', 'Value-Brok']
        
        df = df.drop(columns=[col for col in delete_column_names if col in df.columns])
        
        # Clean SYS18 and SYS27 codes
        df['Bought Code'] = df['Bought Code'].astype(str)
        df['Sold Code'] = df['Sold Code'].astype(str)
        
        # Remove SYS18 and SYS27 from Bought Code
        mask = df['Bought Code'].isin(['SYS18', 'SYS27'])
        df.loc[mask, ['Bought Code', 'Bought Name', 'Bought Quantity']] = None
        
        # Remove SYS18 and SYS27 from Sold Code
        mask = df['Sold Code'].isin(['SYS18', 'SYS27'])
        df.loc[mask, ['Sold Code', 'Sold Name', 'Sold Quantity']] = None
        
        # Convert quantity columns to numeric (handling commas)
        df['Bought Quantity'] = pd.to_numeric(
            df['Bought Quantity'].astype(str).str.replace(',', ''), 
            errors='coerce'
        )
        df['Sold Quantity'] = pd.to_numeric(
            df['Sold Quantity'].astype(str).str.replace(',', ''), 
            errors='coerce'
        )
        
        # Convert Value column to numeric (handling commas)
        # Check if 'Mkt. Value' or 'Value' column exists
        value_column = 'Mkt. Value' if 'Mkt. Value' in df.columns else 'Value'
        df[value_column] = pd.to_numeric(
            df[value_column].astype(str).str.replace(',', ''), 
            errors='coerce'
        )
        
        # Convert Sold Quantity to negative
        df['Sold Quantity'] = df['Sold Quantity'].apply(
            lambda x: -abs(x) if pd.notnull(x) else x
        )
        
        # IMPORTANT: Don't change the sign of Value column
        # The Value column already has correct signs (+ for bought, - for sold)
        # Your original code was making it double negative for sold items
        
        # Merge bought and sold columns
        df['Bought Code'] = df['Bought Code'].fillna(df['Sold Code'])
        df['Bought Name'] = df['Bought Name'].fillna(df['Sold Name'])
        df['Bought Quantity'] = df['Bought Quantity'].fillna(df['Sold Quantity'])
        
        # For merged sold items, the Value column should remain as is (already negative)
        
        # Drop sold columns
        df = df.drop(columns=['Sold Code', 'Sold Name', 'Sold Quantity'])
        
        # Group and aggregate
        summary = (
            df.groupby(['Bought Name', 'Scrip Name', 'Bought Code'], dropna=False)
              .agg({
                  'Bought Quantity': 'sum',
                  value_column: 'sum'
              })
              .reset_index()
        )
        
        # Rename Value column for consistency
        summary = summary.rename(columns={value_column: 'Value'})
        
        # Filter for large transactions - FIXED THRESHOLD
        summary = summary[
            (summary['Bought Quantity'].abs() > 9999) |
            (summary['Value'].abs() > 999999)
        ]
        
        # Format numeric columns
        summary['Bought Quantity'] = summary['Bought Quantity'].apply(
            lambda x: f"{x:,.2f}" if pd.notnull(x) else x
        )
        summary['Value'] = summary['Value'].apply(
            lambda x: f"{x:,.2f}" if pd.notnull(x) else x
        )
        
        return summary
        
    except Exception as e:
        raise Exception(f"Error processing file: {str(e)}")

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy', 
        'message': 'Server is running',
        'backend': 'Flask',
        'version': '1.0.0'
    })

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing"""
    try:
        print("Upload endpoint called")
        
        # Check if file was uploaded
        if 'file' not in request.files:
            print("No file part in request")
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        print(f"File received: {file.filename}")
        
        # Check if file was selected
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Check if file is allowed
        if file and allowed_file(file.filename):
            # Secure the filename
            filename = secure_filename(file.filename)
            
            # Create temp directory
            temp_dir = tempfile.mkdtemp()
            upload_path = os.path.join(temp_dir, filename)
            
            # Save uploaded file
            file.save(upload_path)
            print(f"File saved to: {upload_path}")
            
            # Process the file
            print("Processing file...")
            result_df = process_excel_file(upload_path)
            print(f"Processing complete. Records: {len(result_df)}")
            
            # Save result to CSV
            output_filename = f"processed_{filename.rsplit('.', 1)[0]}.csv"
            output_path = os.path.join(temp_dir, output_filename)
            result_df.to_csv(output_path, index=False)
            print(f"Result saved to: {output_path}")
            
            # Get stats for response
            stats = {
                'total_records': len(result_df),
                'filename': output_filename,
                'columns': list(result_df.columns),
                'preview': result_df.head(5).fillna('').to_dict('records')
            }
            
            response = {
                'success': True,
                'message': 'File processed successfully',
                'stats': stats,
                'download_url': f'/api/download/{output_filename}'
            }
            
            print("Sending response...")
            return jsonify(response)
        
        return jsonify({'error': 'Invalid file type. Only .xlsx files are allowed'}), 400
        
    except Exception as e:
        print(f"Error: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Processing failed: {str(e)}'}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Serve the processed file for download"""
    try:
        # Sanitize filename
        filename = secure_filename(filename)
        temp_dir = tempfile.gettempdir()
        
        # Look for the file in temp directory
        file_path = None
        for root, dirs, files in os.walk(temp_dir):
            if filename in files:
                file_path = os.path.join(root, filename)
                break
        
        if file_path and os.path.exists(file_path):
            print(f"Serving file: {file_path}")
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            print(f"File not found: {filename}")
            return jsonify({'error': 'File not found'}), 404
            
    except Exception as e:
        print(f"Download error: {str(e)}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

def create_app():
    return app


if __name__ == "__main__":
    app.run(debug=True, port=5002)