from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates, IndexingCounties
from utils import format_error

county_mgmt_bp = Blueprint('county_mgmt', __name__)

@county_mgmt_bp.route('/api/admin/counties/list/<int:state_id>', methods=['GET'])
@login_required
def list_counties_by_id(state_id):
    if current_user.role != 'admin': return jsonify([])
    
    # Lookup State FIPS from ID
    state = db.session.get(IndexingStates, state_id)
    if not state: return jsonify([])

    c_list = IndexingCounties.query.filter_by(state_fips=state.fips_code).order_by(IndexingCounties.county_name).all()
    
    return jsonify([{
        'id': c.id, 
        'name': c.county_name, 
        'status': c.is_enabled  # "status" maps to Map Visibility
    } for c in c_list])

@county_mgmt_bp.route('/api/admin/counties/toggle', methods=['POST'])
@login_required
def toggle_county():
    if current_user.role != 'admin': return jsonify({'success': False})
    
    data = request.json
    try:
        c = db.session.get(IndexingCounties, data.get('id'))
        if c:
            c.is_enabled = bool(data.get('status'))
            # When enabling visibility, also enable active status for users
            c.is_active = c.is_enabled 
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'County not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})