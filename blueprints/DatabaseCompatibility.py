import json
from flask import Blueprint, request, jsonify, Response, stream_with_context, current_app
from flask_login import login_required, current_user
from extensions import db
from sqlalchemy import text
from utils import format_error

db_compat_bp = Blueprint('db_compat', __name__)

# [GSI_BLOCK: db_compat_gen]
def generate_compat_sql(data):
    """Generates SQL to change compatibility level."""
    level = data.get('level', '150')
    
    # Get current DB name safely
    try:
        db_name = db.session.execute(text("SELECT DB_NAME()")).scalar()
    except:
        db_name = "TargetDatabase"

    yield "-- DATABASE COMPATIBILITY LEVEL UPDATE\n"
    yield f"-- Target Database: {db_name}\n"
    yield f"-- Target Level: {level}\n\n"

    yield f"USE [master];\nGO\n"
    yield f"ALTER DATABASE [{db_name}] SET COMPATIBILITY_LEVEL = {level};\nGO\n"
    yield f"USE [{db_name}];\nGO\n"
# [GSI_END: db_compat_gen]

@db_compat_bp.route('/api/tools/db-compat/preview', methods=['POST'])
@login_required
def preview_compat():
    # [GSI_BLOCK: db_compat_preview]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json or {}
    
    # DEBUG CHECK: Hide SQL if debug is OFF
    if not data.get('debug'):
        return jsonify({'success': True, 'sql': '-- Preview Hidden. Enable Debug Mode to view SQL generation.'})

    try:
        sql = "".join(list(generate_compat_sql(data)))
        return jsonify({'success': True, 'sql': sql})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: db_compat_preview]

@db_compat_bp.route('/api/tools/db-compat/download-sql', methods=['POST'])
@login_required
def download_compat_sql():
    # [GSI_BLOCK: db_compat_download]
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    data = request.json or {}
    return Response(stream_with_context(generate_compat_sql(data)), mimetype='application/sql', 
                   headers={'Content-Disposition': 'attachment; filename=Set_Compatibility.sql'})
    # [GSI_END: db_compat_download]

@db_compat_bp.route('/api/tools/db-compat/run', methods=['POST'])
@login_required
def run_compat():
    # [GSI_BLOCK: db_compat_run]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json or {}
    level = data.get('level', '150')

    try:
        # Get current DB name
        db_name = db.session.execute(text("SELECT DB_NAME()")).scalar()
        
        # Execute
        sql = f"ALTER DATABASE [{db_name}] SET COMPATIBILITY_LEVEL = {level}"
        db.session.execute(text(sql))
        db.session.commit()
        
        return jsonify({'success': True, 'message': f"Compatibility level set to {level} for {db_name}."})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})
    # [GSI_END: db_compat_run]