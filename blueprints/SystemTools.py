from flask import Blueprint, jsonify, request, session, current_app
from flask_login import login_required, current_user
import os, re
from models import Users

sys_bp = Blueprint('system', __name__)

# --- DEBUG TOGGLE ---
@sys_bp.route('/api/admin/debug/status', methods=['GET'])
@login_required
def get_debug_status():
    return jsonify({'debug_mode': session.get('debug_mode', False)})

@sys_bp.route('/api/admin/debug/toggle', methods=['POST'])
@login_required
def toggle_debug():
    if current_user.role != 'admin': return jsonify({'success': False})
    session['debug_mode'] = not session.get('debug_mode', False)
    return jsonify({'success': True, 'debug_mode': session['debug_mode']})

@sys_bp.route('/api/user/debug/auth-enable', methods=['POST'])
@login_required
def enable_debug_with_auth():
    username = request.json.get('username')
    password = request.json.get('password')
    admin_user = Users.query.filter_by(username=username).first()
    
    if admin_user and admin_user.role == 'admin' and admin_user.check_password(password):
        session['debug_mode'] = True
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Invalid Credentials'})

@sys_bp.route('/api/user/debug/disable', methods=['POST'])
@login_required
def disable_user_debug():
    session['debug_mode'] = False
    return jsonify({'success': True})

# --- SQL VIEWER ---
@sys_bp.route('/api/admin/view-code', methods=['POST'])
@login_required
def view_code():
    if current_user.role != 'admin': return jsonify({'success': False})
    tool_key = request.json.get('tool')
    
    # Map tool keys to filenames
    file_map = {
        'drive': 'blueprints/OpenDriveConnection.py',
        'compat': 'blueprints/DatabaseCompatibility.py',
        'procedures': 'blueprints/SetupDatabaseProcedures.py',
        'edata': 'blueprints/SetupEDataTable.py',
        'keli': 'blueprints/SetupKeliTables.py',
        'seed': 'blueprints/StateManagement.py', # Moved here
        'unindexed': 'blueprints/UnindexedImages.py' 
    }
    
    filename = file_map.get(tool_key)
    if not filename: return jsonify({'success': False, 'message': 'Unknown Tool'})
    
    try:
        file_path = os.path.join(current_app.root_path, filename)
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        
        # Simple extraction logic
        extracted_sql = []
        lines = content.split('\n')
        for line in lines:
            clean = line.strip()
            if (clean.startswith('sql =') or clean.startswith('cmd =') or 'text(' in clean) and any(k in clean.upper() for k in ['ALTER ', 'SELECT ', 'INSERT ', 'UPDATE ', 'DELETE ', 'CREATE ']):
                clean = clean.replace('sql = f"', '').replace('sql = "', '').replace('"', '').replace('text(', '').replace(')', '')
                extracted_sql.append(clean.strip())
                
        final = "\n\n".join(extracted_sql) if extracted_sql else "-- No explicit SQL found in this file"
        return jsonify({'success': True, 'filename': filename, 'code': final})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})