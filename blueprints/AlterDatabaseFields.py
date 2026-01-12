from flask import Blueprint, jsonify, request, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from utils import format_error

# Renamed Blueprint
alter_db_bp = Blueprint('alter_db_fields', __name__)

# 1. DEFINE THE RENAMES (Old Name -> New Name)
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
# instTypeOriginal added FIRST as requested
ADD_COLUMNS_SQL = [
    ("instTypeOriginal", "varchar(1000)", "--Add instTypeOriginal To GenericDataImport"),
    ("instrumentid", "int", "--Add instrumentid To GenericDataImport"),
    ("deleteFlag", "varchar(1000)", "--Add deleteFlag To GenericDataImport"),
    ("change_script_locations", "varchar(1000)", "--Add change_script_locations To GenericDataImport"),
    ("keyOriginalValue", "varchar(1000)", "--Add keyOriginalValue To GenericDataImport")
]

def generate_safe_sql():
    """Generates the SQL script dynamically based on current DB state."""
    sql_statements = []
    
    # Check current columns in DB
    inspector = db.inspect(db.engine)
    existing_columns = [c['name'].lower() for c in inspector.get_columns('GenericDataImport')]
    
    sql_statements.append("-- =============================================")
    sql_statements.append("-- 1. COLUMN RENAMES (sp_rename)")
    sql_statements.append("-- =============================================")

    for old_col, new_col in COLUMN_MAPPINGS.items():
        if old_col.lower() in existing_columns and new_col.lower() not in existing_columns:
            cmd = f"EXEC sp_rename 'GenericDataImport.{old_col}', '{new_col}', 'COLUMN';"
            sql_statements.append(cmd)
        elif new_col.lower() in existing_columns:
            sql_statements.append(f"-- SKIPPING: {old_col} -> {new_col} (Target column already exists)")
        else:
            sql_statements.append(f"-- SKIPPING: {old_col} (Source column not found)")

    sql_statements.append("\n-- =============================================")
    sql_statements.append("-- 2. ADD NEW COLUMNS")
    sql_statements.append("-- =============================================")

    for col_name, col_type, comment in ADD_COLUMNS_SQL:
        if col_name.lower() not in existing_columns:
            sql_statements.append(f"{comment}")
            sql_statements.append(f"ALTER TABLE GenericDataImport ADD {col_name} {col_type};")
        else:
            sql_statements.append(f"-- SKIPPING: {col_name} (Column already exists)")

    return sql_statements

# --- UPDATED ROUTES ---

@alter_db_bp.route('/api/admin/alter-db/preview', methods=['GET'])
@login_required
def preview_schema_changes():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        statements = generate_safe_sql()
        return jsonify({'success': True, 'sql': "\n".join(statements)})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@alter_db_bp.route('/api/admin/alter-db/execute', methods=['POST'])
@login_required
def execute_schema_changes():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    debug_mode = request.json.get('debug', False)
    
    try:
        statements = generate_safe_sql()
        executed_count = 0
        
        commands_to_run = [s for s in statements if not s.strip().startswith('--') and s.strip()]
        
        if not commands_to_run:
            return jsonify({'success': True, 'message': 'No changes needed. Database is up to date.'})

        for cmd in commands_to_run:
            db.session.execute(text(cmd))
            executed_count += 1
            
        db.session.commit()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully executed {executed_count} schema changes.',
            'debug_log': statements if debug_mode else None
        })

    except Exception as e:
        db.session.rollback()
        msg = format_error(e) if debug_mode else "Update failed. Check logs."
        return jsonify({'success': False, 'message': msg})