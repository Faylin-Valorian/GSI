import os, re
from flask import Blueprint, jsonify, request, session, current_app
from flask_login import login_required, current_user

sys_bp = Blueprint('system', __name__)

@sys_bp.route('/api/admin/debug/toggle', methods=['POST'])
@login_required
def toggle_debug():
    if current_user.role != 'admin': return jsonify({'success': False})
    session['debug_mode'] = not session.get('debug_mode', False)
    return jsonify({'success': True, 'mode': session['debug_mode']})

@sys_bp.route('/api/admin/view-code', methods=['POST'])
@login_required
def view_code():
    # ... Paste the view_tool_code logic from geospatial.py here ...
    pass