import os
import json
import sqlalchemy
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text, inspect
from extensions import db

alter_db_bp = Blueprint('alter_db', __name__)

CONFIG_FILE = 'alter_db_config.json'

# --- FACTORY DEFAULTS (Your Standard Logic) ---
# These apply ONLY if the user hasn't saved a custom config for these fields.
DEFAULT_RENAMES = {
    'col01other': 'key_id',
    'col20other': 'legacy_ref' # Example: Add your other standard renames here
}

DEFAULT_NEW_FIELDS = [
    {'name': 'instrumentid', 'type': 'INT IDENTITY(1,1)', 'default': ''},
    {'name': 'change_script_locations', 'type': 'VARCHAR(MAX)', 'default': ''},
    {'name': 'instTypeOriginal', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'keyOriginalValue', 'type': 'VARCHAR(255)', 'default': ''},
    {'name': 'deleteFlag', 'type': 'VARCHAR(10)', 'default': "'FALSE'"}
]

def load_config():
    """Loads user preferences from JSON, or returns empty dict."""
    path = os.path.join(current_app.root_path, CONFIG_FILE)
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config(config_data):
    """Saves user preferences to JSON."""
    path = os.path.join(current_app.root_path, CONFIG_FILE)
    with open(path, 'w') as f:
        json.dump(config_data, f, indent=4)

@alter_db_bp.route('/api/tools/alter-db/init', methods=['GET'])
@login_required
def init_tool():
    """
    1. Inspects GenericDataImport table schema.
    2. Merges with Saved Config (User Prefs).
    3. Merges with Factory Defaults (if no user pref exists).
    """
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    inspector = inspect(db.engine)
    if not inspector.has_table('GenericDataImport'):
        return jsonify({'success': False, 'error': 'Table GenericDataImport not found.'})

    columns = inspector.get_columns('GenericDataImport')
    # columns is list of dicts: {'name': 'id', 'type': INTEGER(), ...}
    
    saved_config = load_config()
    saved_renames = saved_config.get('renames', {})
    saved_adds = saved_config.get('adds', [])

    # 1. Build Field List (Existing Columns)
    field_list = []
    for col in columns:
        orig_name = col['name']
        
        # Determine Target Name: Saved Config > Factory Default > Original Name
        if orig_name in saved_renames:
            target_name = saved_renames[orig_name]
        elif orig_name in DEFAULT_RENAMES:
            target_name = DEFAULT_RENAMES[orig_name]
        else:
            target_name = orig_name
            
        field_list.append({
            'original': orig_name,
            'current': target_name,
            'type': str(col['type']),
            'is_existing': True
        })

    # 2. Build Added Fields List
    # If user has NEVER saved config, show Factory Defaults.
    # If user HAS saved config, show only what they saved (even if empty).
    if not saved_config and not saved_adds:
        # Check if these columns already exist in DB to avoid dupes in UI
        existing_names = [c['name'].lower() for c in columns]
        adds_list = [f for f in DEFAULT_NEW_FIELDS if f['name'].lower() not in existing_names]
    else:
        adds_list = saved_adds

    return jsonify({
        'success': True,
        'fields': field_list,
        'new_fields': adds_list
    })

@alter_db_bp.route('/api/tools/alter-db/preview', methods=['POST'])
@login_required
def preview_sql():
    data = request.json
    renames = data.get('renames', {}) # dict: { 'col01other': 'key_id' }
    new_fields = data.get('new_fields', []) # list of dicts
    
    # Save this configuration for next time
    save_config({'renames': renames, 'adds': new_fields})

    sql_script = "-- DYNAMIC ALTER SCRIPT\n"
    sql_script += "-- Auto-generated based on user inputs\n\n"
    
    # 1. Renames
    if renames:
        sql_script += "-- 1. Renaming Columns\n"
        for old, new in renames.items():
            if old != new:
                # Basic sanitization
                clean_old = old.replace("'", "")
                clean_new = new.replace("'", "")
                sql_script += f"IF EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'{clean_old}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
                sql_script += f"BEGIN\n    EXEC sp_rename 'GenericDataImport.{clean_old}', '{clean_new}', 'COLUMN';\nEND\nGO\n\n"

    # 2. Additions
    if new_fields:
        sql_script += "-- 2. Adding New Fields\n"
        for field in new_fields:
            fname = field['name'].replace("'", "")
            ftype = field['type'].replace(";", "") # Basic SQL injection prevention
            fdefault = field.get('default', '')
            
            sql_script += f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'{fname}' AND Object_ID = Object_ID(N'GenericDataImport'))\n"
            sql_script += f"BEGIN\n    ALTER TABLE GenericDataImport ADD [{fname}] {ftype};\n"
            if fdefault and 'IDENTITY' not in ftype.upper():
                 sql_script += f"    ALTER TABLE GenericDataImport ADD CONSTRAINT [DF_GDI_{fname}] DEFAULT {fdefault} FOR [{fname}];\n"
            sql_script += "END\nGO\n\n"

    return jsonify({'success': True, 'sql': sql_script})

@alter_db_bp.route('/api/tools/alter-db/execute', methods=['POST'])
@login_required
def execute_sql():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    # We re-generate the SQL to ensure what is executed matches the config
    # (Or we could pass the SQL from frontend, but regenerating is safer)
    return preview_sql() # logic is identical for generation, just need to execute

# NOTE: Actual execution logic usually requires separating GO statements.
# For this tool, we will simplify: the frontend calls 'preview' to get SQL, 
# then sends that SQL back to a raw execute endpoint, OR we handle it here.
# Let's handle execution here properly.

@alter_db_bp.route('/api/tools/alter-db/run', methods=['POST'])
@login_required
def run_migration():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    # 1. Generate SQL (Reuse logic)
    # We call the preview logic internally or duplicate it. 
    # To keep it simple, let's assume the frontend sends the config, we generate SQL, split by 'GO', and run.
    data = request.json
    renames = data.get('renames', {})
    new_fields = data.get('new_fields', [])
    
    # Update config
    save_config({'renames': renames, 'adds': new_fields})

    try:
        with db.session.begin():
            # Renames
            for old, new in renames.items():
                if old != new:
                    # SQLAlchemy doesn't support GO, so we run statements individually
                    clean_old = old.replace("'", "")
                    clean_new = new.replace("'", "")
                    # Check existence first
                    check_sql = text(f"SELECT 1 FROM sys.columns WHERE Name = :old AND Object_ID = Object_ID(N'GenericDataImport')")
                    if db.session.execute(check_sql, {'old': clean_old}).fetchone():
                        db.session.execute(text(f"EXEC sp_rename 'GenericDataImport.{clean_old}', '{clean_new}', 'COLUMN'"))

            # Additions
            for field in new_fields:
                fname = field['name']
                ftype = field['type']
                check_sql = text(f"SELECT 1 FROM sys.columns WHERE Name = :name AND Object_ID = Object_ID(N'GenericDataImport')")
                if not db.session.execute(check_sql, {'name': fname}).fetchone():
                    db.session.execute(text(f"ALTER TABLE GenericDataImport ADD [{fname}] {ftype}"))
                    
        return jsonify({'success': True, 'message': 'Schema updated successfully.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})