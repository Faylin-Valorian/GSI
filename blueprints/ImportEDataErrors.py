import os
import json
import time
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties, IndexingStates
from utils import format_error
from werkzeug.utils import secure_filename

import_edata_errors_bp = Blueprint('import_edata_errors', __name__)

# Standard Schema
CREATE_TABLE_SQL = """
CREATE TABLE [{table_name}] (
    [ID] varchar(255), [FN] varchar(255), [OriginalValue] varchar(max),
    [col01varchar] varchar(255), [col02varchar] varchar(255), [col03varchar] varchar(255),
    [col04varchar] varchar(255), [col05varchar] varchar(255), [col06varchar] varchar(255),
    [col07varchar] varchar(255), [col08varchar] varchar(255), [col09varchar] varchar(255),
    [col10varchar] varchar(255), [key_id] varchar(255), [book] varchar(255),
    [page_number] varchar(255), [stech_image_path] varchar(max), [keli_image_path] varchar(max),
    [beginning_page] varchar(255), [ending_page] varchar(255), [record_series_internal_id] varchar(255),
    [record_series_external_id] varchar(255), [instrument_type_internal_id] varchar(255),
    [instrument_type_external_id] varchar(255), [grantor_suffix_internal_id] varchar(255),
    [grantee_suffix_internal_id] varchar(255), [manual_page_count] varchar(255),
    [legal_type] varchar(255), [addition_internal_id] varchar(255), [addition_external_id] varchar(255),
    [township_range_internal_id] varchar(255), [township_range_external_id] varchar(255),
    [col20other] varchar(255), [uf1] varchar(255), [uf2] varchar(255), [uf3] varchar(255),
    [leftovers] varchar(max), [instTypeOriginal] varchar(255), [keyOriginalValue] varchar(255),
    [deleteFlag] varchar(50), [change_script_locations] varchar(max), [instrumentid] varchar(50),
    [checked] varchar(50)
)
"""

@import_edata_errors_bp.route('/api/tools/import-edata-errors/download-sql', methods=['GET'])
@login_required
def download_sql():
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    
    county_id = request.args.get('county_id')
    c = db.session.get(IndexingCounties, county_id)
    if not c: return Response("County not found", 404)
    
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    if not s: return Response("State not found", 404)
    state_abbr = s.state_abbr if s.state_abbr else s.state_name[:2].upper()
    
    base_path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'eData Errors')
    
    def generate():
        yield f"-- GSI IMPORT EDATA ERRORS SCRIPT\n-- County: {c.county_name}\n-- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        if not os.path.exists(base_path):
            yield "-- Error: Source directory not found.\n"
            return
            
        files = [f for f in os.listdir(base_path) if f.lower().endswith('.csv')]
        for filename in files:
            clean_name = os.path.splitext(filename)[0].replace(' ', '_').replace('-', '_')
            table_name = f"{state_abbr}_{c.county_name}_eData_Errors_{clean_name}"
            file_full_path = os.path.join(base_path, filename)
            
            yield f"-- File: {filename}\n"
            yield f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]\n"
            yield CREATE_TABLE_SQL.format(table_name=table_name) + "\n"
            yield f"BULK INSERT [{table_name}] FROM '{file_full_path}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK)\nGO\n\n"

    filename = f"Import_Errors_{c.county_name}.sql"
    return Response(stream_with_context(generate()), mimetype='application/sql', headers={'Content-Disposition': f'attachment; filename={filename}'})

@import_edata_errors_bp.route('/api/tools/import-edata-errors/preview', methods=['POST'])
@login_required
def preview_import():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    if not s: return jsonify({'success': False, 'message': 'State not found'})

    state_abbr = s.state_abbr if s.state_abbr else s.state_name[:2].upper()
    
    base_path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'eData Errors')
    
    if not os.path.exists(base_path):
        return jsonify({'success': False, 'message': 'Directory not found'})
        
    files = [f for f in os.listdir(base_path) if f.lower().endswith('.csv')]
    if not files:
        return jsonify({'success': True, 'sql': '-- No .csv files found in eData Errors directory.'})
        
    sql_output = f"-- PREVIEW: Found {len(files)} files to import\n\n"
    
    for filename in files:
        clean_name = os.path.splitext(filename)[0].replace(' ', '_').replace('-', '_')
        table_name = f"{state_abbr}_{c.county_name}_eData_Errors_{clean_name}"
        file_full_path = os.path.join(base_path, filename)
        
        sql_output += f"-- File: {filename}\n"
        sql_output += f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]\n"
        sql_output += CREATE_TABLE_SQL.format(table_name=table_name) + "\n"
        sql_output += f"BULK INSERT [{table_name}] FROM '{file_full_path}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK)\nGO\n\n"
        
    return jsonify({'success': True, 'sql': sql_output})

@import_edata_errors_bp.route('/api/tools/import-edata-errors/init', methods=['POST'])
@login_required
def init_import():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    county_id = data.get('county_id')
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False, 'message': 'County not found'})
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        
        path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'eData Errors')
        files = []
        if os.path.exists(path):
            files = [f for f in os.listdir(path) if f.lower().endswith('.csv')]
            
        return jsonify({'success': True, 'path': path, 'file_count': len(files), 'files': files})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@import_edata_errors_bp.route('/api/tools/import-edata-errors/execute', methods=['POST'])
@login_required
def execute_import():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    county_id = data.get('county_id')
    debug_mode = data.get('debug', False) # Check Debug Flag
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    
    # Validation: Ensure State Abbr exists
    state_abbr = s.state_abbr if s.state_abbr else s.state_name[:2].upper()
    
    base_path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'eData Errors')
    
    def generate_stream():
        yield json.dumps({'type': 'start', 'message': f'Starting Import for {c.county_name}...'}) + '\n'
        
        if debug_mode:
            yield json.dumps({'type': 'log', 'message': f"DEBUG: State Abbr [{state_abbr}]"}) + '\n'
            yield json.dumps({'type': 'log', 'message': f"DEBUG: Source Path [{base_path}]"}) + '\n'

        if not os.path.exists(base_path):
            yield json.dumps({'type': 'error', 'message': 'Directory not found.'}) + '\n'
            return

        files = [f for f in os.listdir(base_path) if f.lower().endswith('.csv')]
        total = len(files)
        
        if total == 0:
            yield json.dumps({'type': 'complete', 'message': 'No CSV files found.'}) + '\n'
            return

        # Using explicit connection for raw SQL control
        engine = db.engine
        connection = engine.raw_connection()
        cursor = connection.cursor()

        try:
            for i, filename in enumerate(files):
                # 1. Naming Convention: [StateAbbr]_[County]_eData_Errors_[File]
                # Clean filename: remove extension, replace spaces/dashes
                clean_name = os.path.splitext(filename)[0].replace(' ', '_').replace('-', '_')
                table_name = f"{state_abbr}_{c.county_name}_eData_Errors_{clean_name}"
                
                if debug_mode:
                    yield json.dumps({'type': 'log', 'message': f"DEBUG: Target Table -> [{table_name}]"}) + '\n'

                # 2. Drop & Create
                drop_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"
                create_sql = CREATE_TABLE_SQL.format(table_name=table_name)
                
                cursor.execute(drop_sql)
                cursor.execute(create_sql)
                connection.commit() # Commit schema change

                # 3. Bulk Insert
                file_full_path = os.path.join(base_path, filename)
                bulk_sql = f"""
                    BULK INSERT [{table_name}]
                    FROM '{file_full_path}'
                    WITH (
                        FORMAT = 'CSV',
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        TABLOCK
                    )
                """
                
                if debug_mode:
                    yield json.dumps({'type': 'log', 'message': f"DEBUG: Executing Bulk Insert on {filename}..."}) + '\n'

                try:
                    cursor.execute(bulk_sql)
                    connection.commit() # Commit data
                    row_count = cursor.rowcount
                    
                    msg = f"Imported {filename} ({row_count} rows)"
                    if debug_mode:
                        yield json.dumps({'type': 'log', 'message': f"SUCCESS: {msg}"}) + '\n'
                        
                except Exception as sql_err:
                    connection.rollback()
                    err_msg = f"SQL Error on {filename}: {str(sql_err)}"
                    yield json.dumps({'type': 'log', 'message': err_msg}) + '\n'
                    yield json.dumps({'type': 'error', 'message': err_msg}) + '\n'

                percent = int(((i + 1) / total) * 100)
                yield json.dumps({'type': 'progress', 'percent': percent, 'message': f"Importing {filename}..."}) + '\n'
                time.sleep(0.1)

            yield json.dumps({'type': 'complete', 'message': 'Import Process Completed.'}) + '\n'

        except Exception as e:
            yield json.dumps({'type': 'error', 'message': str(e)}) + '\n'
        finally:
            cursor.close()
            connection.close()

    return Response(stream_with_context(generate_stream()), mimetype='application/json')