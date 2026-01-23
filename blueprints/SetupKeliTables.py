import os
import json
import csv
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from werkzeug.utils import secure_filename
from extensions import db
from models import IndexingCounties, IndexingStates
from utils import format_error

setup_keli_bp = Blueprint('setup_keli', __name__)

@setup_keli_bp.route('/api/tools/setup-keli/download-sql', methods=['GET'])
@login_required
def download_sql():
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    
    county_id = request.args.get('county_id')
    c = db.session.get(IndexingCounties, county_id)
    if not c: return Response("County not found", 404)
    
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    if not s: return Response("State not found", 404)

    base_folder = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'Keli Files')
    c_clean = secure_filename(c.county_name)

    def generate():
        yield f"-- GSI KELI IMPORT SCRIPT\n-- County: {c.county_name}\n\n"
        
        for root, dirs, files in os.walk(base_folder):
            for file in files:
                if file.lower().endswith('.csv'):
                    full_path = os.path.join(root, file)
                    filename = os.path.basename(full_path)
                    raw_name = os.path.splitext(filename)[0]
                    safe_raw = "".join([c for c in raw_name if c.isalnum() or c in ('_')])
                    table_name = f"{c_clean}_keli_{safe_raw}"
                    
                    try:
                        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                            header = f.readline().strip()
                            rdr = csv.reader([header])
                            cols = next(rdr)
                        cols_def = ", ".join([f"[{h.strip()}] VARCHAR(MAX)" for h in cols if h.strip()])
                        yield f"-- File: {filename}\nDROP TABLE IF EXISTS [dbo].[{table_name}];\nCREATE TABLE [dbo].[{table_name}] ({cols_def});\n"
                        yield f"BULK INSERT [dbo].[{table_name}] FROM '{full_path}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDQUOTE = '\"', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);\nGO\n\n"
                    except Exception as e:
                        yield f"-- Error: {str(e)}\n"

    filename = f"Setup_Keli_{c.county_name}.sql"
    return Response(stream_with_context(generate()), mimetype='application/sql', headers={'Content-Disposition': f'attachment; filename={filename}'})

@setup_keli_bp.route('/api/tools/setup-keli/preview', methods=['POST'])
@login_required
def preview_keli_import():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    
    county = db.session.get(IndexingCounties, county_id)
    if not county: return jsonify({'success': False, 'message': 'County not found'})
    
    state = IndexingStates.query.filter_by(fips_code=county.state_fips).first()
    if not state: return jsonify({'success': False, 'message': 'State not found'})
    
    s_clean = secure_filename(state.state_name)
    c_clean = secure_filename(county.county_name)
    base_folder = os.path.join(current_app.root_path, 'data', s_clean, c_clean, 'Keli Files')
    abs_folder_path = os.path.abspath(base_folder)
    
    if not os.path.exists(abs_folder_path): return jsonify({'success': False, 'message': 'Folder not found'})
    
    # Recursive Scan (os.walk)
    csv_files = []
    for root, dirs, files in os.walk(abs_folder_path):
        for file in files:
            if file.lower().endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    if not csv_files: return jsonify({'success': True, 'sql': '-- No CSV files found'})
    
    sql_output = f"-- PREVIEW: Found {len(csv_files)} files\n\n"
    
    for full_path in csv_files:
        filename = os.path.basename(full_path)
        raw_name = os.path.splitext(filename)[0]
        safe_raw = "".join([c for c in raw_name if c.isalnum() or c in ('_')])
        table_name = f"{c_clean}_keli_{safe_raw}"
        full_table_name = f"[dbo].[{table_name}]"
        
        try:
            # Read only header for preview
            with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                header_line = f.readline().strip()
                reader = csv.reader([header_line])
                headers = next(reader)
            
            cols_def = ", ".join([f"[{h.strip()}] VARCHAR(MAX)" for h in headers if h.strip()])
            sql_output += f"-- File: {filename}\nDROP TABLE IF EXISTS {full_table_name};\nCREATE TABLE {full_table_name} ({cols_def});\n"
            sql_output += f"BULK INSERT {full_table_name} FROM '{full_path}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDQUOTE = '\"', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK);\nGO\n\n"
        except Exception as e:
            sql_output += f"-- Error reading {filename}: {str(e)}\n\n"
            
    return jsonify({'success': True, 'sql': sql_output})

@setup_keli_bp.route('/api/tools/setup-keli', methods=['POST'])
@login_required
def run_keli_import():
    req_data = request.json
    user_role = current_user.role

    def generate():
        # 1. Security & Validation
        if user_role != 'admin':
            yield json.dumps({'type': 'error', 'message': 'Unauthorized'}) + '\n'
            return

        county_id = req_data.get('county_id')
        if not county_id:
            yield json.dumps({'type': 'error', 'message': 'Context missing'}) + '\n'
            return

        county = db.session.get(IndexingCounties, county_id)
        if not county:
             yield json.dumps({'type': 'error', 'message': 'County not found'}) + '\n'
             return
             
        state = IndexingStates.query.filter_by(fips_code=county.state_fips).first()
        
        if not state:
            yield json.dumps({'type': 'error', 'message': 'State not found'}) + '\n'
            return

        # 2. Path Resolution
        s_clean = secure_filename(state.state_name)
        c_clean = secure_filename(county.county_name)
        
        # Target: /data/{State}/{County}/Keli Files
        base_folder = os.path.join(current_app.root_path, 'data', s_clean, c_clean, 'Keli Files')
        abs_folder_path = os.path.abspath(base_folder)

        if not os.path.exists(abs_folder_path):
            yield json.dumps({'type': 'error', 'message': 'Keli Files folder not found.'}) + '\n'
            return

        # 3. Scan Files
        csv_files = [f for f in os.listdir(abs_folder_path) if f.lower().endswith('.csv')]
        total_files = len(csv_files)
        
        if total_files == 0:
            yield json.dumps({'type': 'error', 'message': 'No .csv files found in Keli folder.'}) + '\n'
            return

        yield json.dumps({'type': 'progress', 'current': 0, 'total': total_files, 'percent': 0}) + '\n'

        processed_count = 0
        errors = []

        # 4. Process Loop
        for i, filename in enumerate(csv_files):
            full_path = os.path.join(abs_folder_path, filename)
            
            # Construct Table Name: [dbo].[county_keli_filename]
            raw_name = os.path.splitext(filename)[0]
            # Sanitize simple chars only for table name to avoid SQL Injection risks
            safe_raw = "".join([c for c in raw_name if c.isalnum() or c in ('_')])
            table_name = f"{c_clean}_keli_{safe_raw}"
            full_table_name = f"[dbo].[{table_name}]"

            try:
                # --- STEP A: READ HEADER (Python) ---
                with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                    header_line = f.readline().strip()
                    if not header_line:
                        raise Exception("Empty file")
                    
                    # Use CSV reader to handle quoted headers correctly
                    reader = csv.reader([header_line])
                    headers = next(reader)

                if not headers:
                    raise Exception("No headers found")

                # --- STEP B: CREATE TABLE ---
                # Build columns list: [Col1] VARCHAR(MAX), [Col2] VARCHAR(MAX)...
                cols_def = ", ".join([f"[{h.strip()}] VARCHAR(MAX)" for h in headers if h.strip()])
                
                drop_create_sql = f"""
                    DROP TABLE IF EXISTS {full_table_name};
                    CREATE TABLE {full_table_name} ({cols_def});
                """
                db.session.execute(text(drop_create_sql))
                db.session.commit()

                # --- STEP C: BULK INSERT (SQL) ---
                # Escape single quotes in path for SQL (e.g. O'Brien folder)
                sql_safe_path = full_path.replace("'", "''")
                
                # We inject the path directly because BULK INSERT does NOT support parameters (e.g. @Path)
                bulk_sql = f"""
                    BULK INSERT {full_table_name} 
                    FROM '{sql_safe_path}' 
                    WITH (
                        FORMAT = 'CSV', 
                        FIRSTROW = 2, 
                        FIELDQUOTE = '"', 
                        FIELDTERMINATOR = ',', 
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );
                """
                db.session.execute(text(bulk_sql))
                db.session.commit()

                processed_count += 1

            except Exception as e:
                db.session.rollback()
                
                # --- NEW ERROR HANDLING ---
                error_message = format_error(e)
                errors.append(f"{filename}: {error_message}")

            # Yield Progress
            percent = int(((i + 1) / total_files) * 100)
            yield json.dumps({
                'type': 'progress',
                'current': i + 1,
                'total': total_files,
                'percent': percent,
                'filename': filename
            }) + '\n'

        # 5. Final Summary
        if processed_count == 0 and len(errors) > 0:
            yield json.dumps({'type': 'error', 'message': f"All failed. First: {errors[0]}"}) + '\n'
        else:
            msg = f"Processed {processed_count} files."
            if errors: msg += f" ({len(errors)} errors)"
            yield json.dumps({'type': 'complete', 'message': msg}) + '\n'

    return Response(stream_with_context(generate()), mimetype='application/json')