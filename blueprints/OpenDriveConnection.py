import json
from flask import Blueprint, request, jsonify, Response, stream_with_context
from flask_login import login_required, current_user
from extensions import db
from sqlalchemy import text
from utils import format_error

open_drive_bp = Blueprint('open_drive', __name__)

def generate_drive_sql(data):
    """Generates the SQL script to map a network drive via xp_cmdshell."""
    letter = data.get('letter', 'Z:')
    path = data.get('path', '')
    user = data.get('user', '')
    password = data.get('pass', '')

    yield "-- MAP NETWORK DRIVE SCRIPT\n"
    yield "-- Enables xp_cmdshell and maps a drive letter for SQL Server access.\n\n"

    yield "-- 1. Enable Advanced Options & xp_cmdshell\n"
    yield "EXEC sp_configure 'show advanced options', 1;\nRECONFIGURE;\n"
    yield "EXEC sp_configure 'xp_cmdshell', 1;\nRECONFIGURE;\nGO\n\n"

    cmd = f'net use {letter} "{path}"'
    if password:
        cmd += f' {password}'
    if user:
        cmd += f' /user:{user}'
    cmd += ' /persistent:yes'

    yield f"-- 2. Map Drive ({letter})\n"
    yield f"-- Command: {cmd.replace(password, '******') if password else cmd}\n"
    yield f"EXEC xp_cmdshell '{cmd}';\nGO\n"

    yield "\n-- 3. Verify Mapping\n"
    yield f"EXEC xp_cmdshell 'dir {letter}';\nGO\n"

@open_drive_bp.route('/api/tools/open-drive/preview', methods=['POST'])
@login_required
def preview_drive():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json or {}
    try:
        sql = "".join(list(generate_drive_sql(data)))
        return jsonify({'success': True, 'sql': sql})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@open_drive_bp.route('/api/tools/open-drive/download-sql', methods=['POST'])
@login_required
def download_drive_sql():
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    data = request.json or {}
    return Response(stream_with_context(generate_drive_sql(data)), mimetype='application/sql', 
                   headers={'Content-Disposition': 'attachment; filename=Map_Drive.sql'})

@open_drive_bp.route('/api/tools/open-drive/run', methods=['POST'])
@login_required
def run_drive():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json or {}
    
    # Security Note: This executes raw shell commands constructed from input.
    # In a production environment, this should be strictly validated or avoided.
    # Assuming trusted admin use for this tool.
    
    letter = data.get('letter', 'Z:')
    path = data.get('path', '')
    user = data.get('user', '')
    password = data.get('pass', '')

    if not path: return jsonify({'success': False, 'message': 'Network Path is required.'})

    cmd = f'net use {letter} "{path}"'
    if password: cmd += f' {password}'
    if user: cmd += f' /user:{user}'
    cmd += ' /persistent:yes'

    try:
        # 1. Enable xp_cmdshell
        db.session.execute(text("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"))
        db.session.execute(text("EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE;"))
        
        # 2. Map Drive
        # Note: xp_cmdshell output comes back as rows. We just want to execute.
        db.session.execute(text(f"EXEC xp_cmdshell '{cmd}'"))
        db.session.commit()
        
        return jsonify({'success': True, 'message': f"Drive {letter} mapped successfully (or command issued)."})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})