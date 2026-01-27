import os
import json
import sqlalchemy
from flask import Blueprint, request, jsonify, current_app, Response, stream_with_context
from flask_login import login_required, current_user
from sqlalchemy import text, inspect
from extensions import db

alter_db_bp = Blueprint('alter_db', __name__)

# [GSI_BLOCK: alter_db_constants]
CONFIG_FILE = 'alter_db_config.json'

# --- FACTORY DEFAULTS ---
DEFAULT_RENAMES = {
    'col01other': 'key_id',
    'col02other': 'book',
    'col03other': 'page_number',
    'col04other': 'stech_image_path',
    'col05other': 'keli_image_path',
    'col06other': 'beginning_page',
    'col07other': 'ending_page',
    'col08other': 'record_series_internal_id',
    'col09other': 'record_series_external_id',
    'col10other': 'instrument_type_internal_id',
    'col11other': 'instrument_type_external_id',
    'col12other': 'grantor_suffix_internal_id',
    'col13other': 'grantee_suffix_internal_id',
    'col14other': 'manual_page_count',
    'col15other': 'legal_type',
    'col16other': 'addition_internal_id',
    'col17other': 'addition_external_id',
    'col18other': 'township_range_internal_id',
    'col19other': 'township_range_external_id',
    'col20other': 'col20other'
}

DEFAULT_NEW_FIELDS = [
    {'name': 'instrumentid', 'type': 'INT', 'default': ''},
    {'name': 'change_script_locations', 'type': 'VARCHAR(MAX)', 'default': ''},
    {'name': 'instTypeOriginal', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'keyOriginalValue', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'deleteFlag', 'type': 'VARCHAR(10)', 'default': ''}
]
# [GSI_END: alter_db_constants]

# [GSI_BLOCK: alter_db_config]
def load_config():
    path = os.path.join(current_app.root_path, CONFIG_FILE)
    if os.path.exists(path):
        try:
            with open(path, 'r') as f: return json.load(f)
        except: pass
    return {}

def save_config(data):
    path = os.path.join(current_app.root_path, CONFIG_FILE)
    with open(path, 'w') as f: json.dump(data, f, indent=4)
# [GSI_END: alter_db_config]

@alter_db_bp.route('/api/tools/alter-db/init', methods=['GET'])
@login_required
def init_tool():
    # [GSI_BLOCK: alter_db_init]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        inspector = inspect(db.engine)
        if not inspector.has_table('GenericDataImport'):
            return jsonify({'success': False, 'error': 'Table GenericDataImport not found.'})

        columns = inspector.get_columns('GenericDataImport')
        saved_config = load_config()
        saved_renames = saved_config.get('renames', {})
        saved_adds = saved_config.get('adds', [])

        # 1. Processing Renames
        field_list = []
        for col in columns:
            orig = col['name']
            orig_lower = orig.lower()
            
            if orig in saved_renames: 
                current = saved_renames[orig]
            elif orig_lower in DEFAULT_RENAMES: 
                current = DEFAULT_RENAMES[orig_lower]
            else: 
                current = orig
            
            field_list.append({'original': orig, 'current': current, 'type': str(col['type'])})

        # 2. Processing New Fields (THE FIX)
        existing_col_names = [c['name'].lower() for c in columns]
        
        # Start with saved user adds
        adds_list = list(saved_adds) 
        
        # Check Factory Defaults: If not in DB and not already in our list, ADD IT
        current_add_names = [a['name'].lower() for a in adds_list]
        
        for df in DEFAULT_NEW_FIELDS:
            # If the default field is NOT in the database AND NOT already queued to be added
            if df['name'].lower() not in existing_col_names and df['name'].lower() not in current_add_names:
                adds_list.append(df)

        return jsonify({'success': True, 'fields': field_list, 'new_fields': adds_list})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    # [GSI_END: alter_db_init]

@alter_db_bp.route('/api/tools/alter-db/preview', methods=['POST'])
@login_required
def preview_sql():
    # [GSI_BLOCK: alter_db_preview]
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    save_config({'renames': renames, 'adds': new_fields})

    sql_parts = ["-- DYNAMIC SCHEMA UPDATE\n"]
    
    # 1. Renames
    rename_ops = []
    for old, new in renames.items():
        if old != new:
            rename_ops.append(f"IF EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{old}' AND Object_ID = Object_ID(N'GenericDataImport'))\nBEGIN\n    EXEC sp_rename 'GenericDataImport.{old}', '{new}', 'COLUMN';\nEND\nGO")

    if rename_ops:
        sql_parts.append("-- 1. Renames")
        sql_parts.extend(rename_ops)
    else:
        sql_parts.append("-- 1. Renames (None Detected)")

    # 2. New Columns
    add_ops = []
    for f in new_fields:
        name, ftype, default = f['name'], f['type'], f.get('default', '')
        op = f"IF NOT EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{name}' AND Object_ID = Object_ID(N'GenericDataImport'))\nBEGIN\n    ALTER TABLE GenericDataImport ADD [{name}] {ftype};\n"
        if default and 'IDENTITY' not in ftype.upper():
            op += f"    ALTER TABLE GenericDataImport ADD CONSTRAINT [DF_GDI_{name}] DEFAULT {default} FOR [{name}];\n"
        op += "END\nGO"
        add_ops.append(op)

    if add_ops:
        sql_parts.append("\n-- 2. New Columns")
        sql_parts.extend(add_ops)
    else:
        sql_parts.append("\n-- 2. New Columns (None Detected)")

    return jsonify({'success': True, 'sql': "\n\n".join(sql_parts)})
    # [GSI_END: alter_db_preview]

@alter_db_bp.route('/api/tools/alter-db/download-sql', methods=['POST'])
@login_required
def download_sql():
    # [GSI_BLOCK: alter_db_download]
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    
    def generate():
        yield "-- GSI SCHEMA UPDATE SCRIPT\n-- Generated: " + str(sqlalchemy.func.now()) + "\n\n"
        
        # Renames
        yield "-- 1. Renames\n"
        for old, new in renames.items():
            if old != new:
                yield f"IF EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{old}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
                yield f"BEGIN\n    EXEC sp_rename 'GenericDataImport.{old}', '{new}', 'COLUMN';\nEND\nGO\n\n"
        
        # New Fields
        yield "-- 2. New Columns\n"
        for f in new_fields:
            name, ftype, default = f['name'], f['type'], f.get('default', '')
            yield f"IF NOT EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{name}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
            yield f"BEGIN\n    ALTER TABLE GenericDataImport ADD [{name}] {ftype};\n"
            if default and 'IDENTITY' not in ftype.upper():
                yield f"    ALTER TABLE GenericDataImport ADD CONSTRAINT [DF_GDI_{name}] DEFAULT {default} FOR [{name}];\n"
            yield "END\nGO\n\n"

    return Response(stream_with_context(generate()), mimetype='application/sql', headers={'Content-Disposition': 'attachment; filename=Schema_Update.sql'})
    # [GSI_END: alter_db_download]

@alter_db_bp.route('/api/tools/alter-db/execute', methods=['POST'])
@login_required
def execute_sql():
    # [GSI_BLOCK: alter_db_execute]
    return jsonify({'success': False, 'message': "Use /run endpoint for execution"}) 
    # [GSI_END: alter_db_execute]

@alter_db_bp.route('/api/tools/alter-db/run', methods=['POST'])
@login_required
def run_migration():
    # [GSI_BLOCK: alter_db_run]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    save_config({'renames': renames, 'adds': new_fields})

    try:
        # Renames
        for old, new in renames.items():
            if old != new:
                chk = text(f"SELECT 1 FROM sys.columns WHERE Name=:o AND Object_ID=Object_ID(N'GenericDataImport')")
                if db.session.execute(chk, {'o': old}).fetchone():
                    db.session.execute(text(f"EXEC sp_rename 'GenericDataImport.{old}', '{new}', 'COLUMN'"))
        
        # New Fields
        for f in new_fields:
            name, ftype = f['name'], f['type']
            chk = text(f"SELECT 1 FROM sys.columns WHERE Name=:n AND Object_ID=Object_ID(N'GenericDataImport')")
            if not db.session.execute(chk, {'n': name}).fetchone():
                db.session.execute(text(f"ALTER TABLE GenericDataImport ADD [{name}] {ftype}"))
        
        db.session.commit()
        return jsonify({'success': True, 'message': "Schema updated successfully."})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: alter_db_run]