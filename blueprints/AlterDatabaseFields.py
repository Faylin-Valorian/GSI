import os
import json
import sqlalchemy
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text, inspect
from extensions import db

alter_db_bp = Blueprint('alter_db', __name__)

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
    'col20other': 'legacy_ref'
}

DEFAULT_NEW_FIELDS = [
    {'name': 'instrumentid', 'type': 'INT IDENTITY(1,1)', 'default': ''},
    {'name': 'change_script_locations', 'type': 'VARCHAR(MAX)', 'default': ''},
    {'name': 'instTypeOriginal', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'keyOriginalValue', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'deleteFlag', 'type': 'VARCHAR(10)', 'default': "'FALSE'"}
]

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

@alter_db_bp.route('/api/tools/alter-db/init', methods=['GET'])
@login_required
def init_tool():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        inspector = inspect(db.engine)
        if not inspector.has_table('GenericDataImport'):
            return jsonify({'success': False, 'error': 'Table GenericDataImport not found.'})

        columns = inspector.get_columns('GenericDataImport')
        saved_config = load_config()
        saved_renames = saved_config.get('renames', {})
        saved_adds = saved_config.get('adds', [])

        field_list = []
        existing_col_names = [c['name'].lower() for c in columns]
        
        for col in columns:
            orig = col['name']
            if orig in saved_renames: current = saved_renames[orig]
            elif orig in DEFAULT_RENAMES: current = DEFAULT_RENAMES[orig]
            else: current = orig
            field_list.append({'original': orig, 'current': current, 'type': str(col['type'])})

        if not saved_config:
            adds_list = [f for f in DEFAULT_NEW_FIELDS if f['name'].lower() not in existing_col_names]
        else:
            adds_list = saved_adds

        return jsonify({'success': True, 'fields': field_list, 'new_fields': adds_list})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@alter_db_bp.route('/api/tools/alter-db/preview', methods=['POST'])
@login_required
def preview_sql():
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    save_config({'renames': renames, 'adds': new_fields})

    sql = "-- DYNAMIC SCHEMA UPDATE\n\n"
    if renames:
        sql += "-- 1. Renames\n"
        for old, new in renames.items():
            if old != new:
                sql += f"IF EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{old}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
                sql += f"BEGIN\n    EXEC sp_rename 'GenericDataImport.{old}', '{new}', 'COLUMN';\nEND\nGO\n\n"
    if new_fields:
        sql += "-- 2. New Columns\n"
        for f in new_fields:
            name, ftype, default = f['name'], f['type'], f.get('default', '')
            sql += f"IF NOT EXISTS(SELECT 1 FROM sys.columns WHERE Name = N'{name}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
            sql += f"BEGIN\n    ALTER TABLE GenericDataImport ADD [{name}] {ftype};\n"
            if default and 'IDENTITY' not in ftype.upper():
                sql += f"    ALTER TABLE GenericDataImport ADD CONSTRAINT [DF_GDI_{name}] DEFAULT {default} FOR [{name}];\n"
            sql += "END\nGO\n\n"
    return jsonify({'success': True, 'sql': sql})

@alter_db_bp.route('/api/tools/alter-db/execute', methods=['POST'])
@login_required
def execute_sql():
    # Use same logic as preview to ensure consistency
    return jsonify({'success': False, 'message': "Use /run endpoint for execution"}) 

@alter_db_bp.route('/api/tools/alter-db/run', methods=['POST'])
@login_required
def run_migration():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    save_config({'renames': renames, 'adds': new_fields})

    try:
        with db.session.begin():
            for old, new in renames.items():
                if old != new:
                    chk = text(f"SELECT 1 FROM sys.columns WHERE Name=:o AND Object_ID=Object_ID(N'GenericDataImport')")
                    if db.session.execute(chk, {'o': old}).fetchone():
                        db.session.execute(text(f"EXEC sp_rename 'GenericDataImport.{old}', '{new}', 'COLUMN'"))
            for f in new_fields:
                name, ftype = f['name'], f['type']
                chk = text(f"SELECT 1 FROM sys.columns WHERE Name=:n AND Object_ID=Object_ID(N'GenericDataImport')")
                if not db.session.execute(chk, {'n': name}).fetchone():
                    db.session.execute(text(f"ALTER TABLE GenericDataImport ADD [{name}] {ftype}"))
        return jsonify({'success': True, 'message': "Schema updated successfully."})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})