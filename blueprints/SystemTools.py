import os
import shutil
import time
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from extensions import db
from models import IndexingCounties, IndexingStates
from werkzeug.utils import secure_filename

sys_bp = Blueprint('system_tools', __name__)

# [GSI_BLOCK: sys_debug_status]
@sys_bp.route('/api/admin/debug/status', methods=['GET'])
@login_required
def debug_status():
    """
    Provides system status for the admin debug poller.
    Fixes the 404 error in the console.
    """
    if current_user.role != 'admin': 
        return jsonify({'active': False, 'message': 'Unauthorized'}), 403
        
    return jsonify({
        'active': True,
        'timestamp': time.time(),
        'status': 'online',
        'debug_mode': current_app.debug
    })
# [GSI_END: sys_debug_status]

# [GSI_BLOCK: sys_folder_check]
@sys_bp.route('/api/admin/tools/folder-check', methods=['POST'])
@login_required
def folder_check():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    results = []
    states = IndexingStates.query.filter_by(is_enabled=True).all()
    
    data_root = os.path.join(current_app.root_path, 'data')
    if not os.path.exists(data_root):
        os.makedirs(data_root)

    for state in states:
        s_path = os.path.join(data_root, secure_filename(state.state_name))
        
        counties = IndexingCounties.query.filter_by(state_fips=state.fips_code).all()
        for county in counties:
            c_path = os.path.join(s_path, secure_filename(county.county_name))
            images_path = os.path.join(c_path, 'Images')
            
            status = 'missing'
            if os.path.exists(images_path):
                status = 'ok'
            elif os.path.exists(c_path):
                status = 'no_images'
                
            results.append({
                'state': state.state_name,
                'county': county.county_name,
                'path': c_path,
                'status': status
            })
            
    return jsonify({'success': True, 'results': results})
# [GSI_END: sys_folder_check]

# [GSI_BLOCK: sys_create_folders]
@sys_bp.route('/api/admin/tools/create-folders', methods=['POST'])
@login_required
def create_folders():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    count_created = 0
    states = IndexingStates.query.filter_by(is_enabled=True).all()
    data_root = os.path.join(current_app.root_path, 'data')

    for state in states:
        s_path = os.path.join(data_root, secure_filename(state.state_name))
        if not os.path.exists(s_path):
            os.makedirs(s_path)
            
        counties = IndexingCounties.query.filter_by(state_fips=state.fips_code).all()
        for county in counties:
            c_path = os.path.join(s_path, secure_filename(county.county_name))
            images_path = os.path.join(c_path, 'Images')
            
            if not os.path.exists(images_path):
                os.makedirs(images_path)
                count_created += 1
                
    return jsonify({'success': True, 'message': f'Created {count_created} missing folder sets.'})
# [GSI_END: sys_create_folders]

# [GSI_BLOCK: sys_restart]
@sys_bp.route('/api/admin/system/restart', methods=['POST'])
@login_required
def system_restart():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    # Trigger restart by touching a watched file
    try:
        app_file = os.path.join(current_app.root_path, 'app.py')
        os.utime(app_file, None)
        return jsonify({'success': True, 'message': 'Restart signal sent.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
# [GSI_END: sys_restart]