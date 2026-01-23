from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates, IndexingCounties
from utils import format_error, ensure_folders

county_mgmt_bp = Blueprint('county_mgmt', __name__)

@county_mgmt_bp.route('/api/admin/counties/list/<int:state_id>', methods=['GET'])
@login_required
def list_counties_by_id(state_id):
    if current_user.role != 'admin': return jsonify([])
    
    state = db.session.get(IndexingStates, state_id)
    if not state: return jsonify([])

    c_list = IndexingCounties.query.filter_by(state_fips=state.fips_code).order_by(IndexingCounties.county_name).all()
    
    return jsonify([{
        'id': c.id, 
        'name': c.county_name, 
        'is_enabled': c.is_enabled, 
        'is_active': c.is_active
    } for c in c_list])

@county_mgmt_bp.route('/api/admin/counties/toggle', methods=['POST'])
@login_required
def toggle_county():
    if current_user.role != 'admin': return jsonify({'success': False})
    
    data = request.json
    try:
        c = db.session.get(IndexingCounties, data.get('id'))
        if c:
            field = data.get('field') # 'active' or 'enabled'
            new_status = bool(data.get('status'))
            
            if field == 'active':
                c.is_active = new_status
            elif field == 'enabled':
                c.is_enabled = new_status
                
                # PATCH: Ensure folders exist if enabling visibility
                if new_status:
                    state = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
                    if state:
                        ensure_folders(state.state_name, c.county_name)
                
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'County not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})