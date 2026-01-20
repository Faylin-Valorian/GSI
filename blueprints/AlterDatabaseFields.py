from flask import Blueprint, jsonify, request, session
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from utils import format_error

alter_db_bp = Blueprint('alter_db_fields', __name__)

# 1. DEFINE MAPPINGS (Old -> New)
COLUMN_MAPPINGS = {
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
    'col19other': 'township_range_external_id'
}

# 2. DEFINE NEW COLUMNS
ADD_COLUMNS_SQL = [
    ("instTypeOriginal", "varchar(1000)"),
    ("instrumentid", "int"),
    ("deleteFlag", "varchar(1000)"),
    ("change_script_locations", "varchar(1000)"),
    ("keyOriginalValue", "varchar(1000)")
]

def generate_safe_sql():
    """Generates the SQL script dynamically based on current DB state."""
    sql_statements = []
    inspector = db.inspect(db.engine)
    existing_columns = [c['name'].lower() for c in inspector.get_columns('GenericDataImport')]
    
    sql_statements.append("-- 1. COLUMN RENAMES")
    for old_col, new_col in COLUMN_MAPPINGS.items():
        if old_col.lower() in existing_columns and new_col.lower() not in existing_columns:
            sql_statements.append(f"EXEC sp_rename 'GenericDataImport.{old_col}', '{new_col}', 'COLUMN';")
        elif new_col.lower() in existing_columns:
            sql_statements.append(f"-- SKIPPING: {old_col} -> {new_col} (Already exists)")

    sql_statements.append("\n-- 2. ADD NEW COLUMNS")
    for col_name, col_type in ADD_COLUMNS_SQL:
        if col_name.lower() not in existing_columns:
            sql_statements.append(f"ALTER TABLE GenericDataImport ADD {col_name} {col_type};")
        else:
            sql_statements.append(f"-- SKIPPING: {col_name} (Exists)")

    return sql_statements

# --- ROUTES ---

@alter_db_bp.route('/api/admin/alter-db/preview', methods=['GET'])
@login_required
def preview_schema_changes():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        statements = generate_safe_sql()
        return jsonify({'success': True, 'sql': "\n".join(statements)})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@alter_db_bp.route('/api/admin/alter-db/execute', methods=['POST'])
@login_required
def execute_schema_changes():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    # Check GLOBAL Debug Setting from Session
    debug_mode = session.get('debug_mode', False)
    
    try:
        statements = generate_safe_sql()
        executed_count = 0
        commands_to_run = [s for s in statements if not s.strip().startswith('--')]
        
        if not commands_to_run:
            return jsonify({'success': True, 'message': 'Database is already up to date.'})

        for cmd in commands_to_run:
            db.session.execute(text(cmd))
            executed_count += 1
            
        db.session.commit()
        
        msg = f"Success: {executed_count} schema changes applied."
        if debug_mode:
            detail = "\n".join([f"• {c[:60]}..." for c in commands_to_run])
            msg = f"DEBUG SUCCESS ({executed_count} changes):\n{detail}"
        
        return jsonify({'success': True, 'message': msg})

    except Exception as e:
        db.session.rollback()
        msg = f"DEBUG ERROR: {format_error(e)}" if debug_mode else "Update failed. Check logs."
        return jsonify({'success': False, 'message': msg})