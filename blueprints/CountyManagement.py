from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from extensions import db
from models import IndexingCounties, IndexingStates
from sqlalchemy import text

county_mgmt_bp = Blueprint('county_mgmt', __name__)

@county_mgmt_bp.route('/api/counties', methods=['GET'])
@login_required
def get_counties():
    # [GSI_BLOCK: cm_get_counties]
    counties = db.session.query(IndexingCounties, IndexingStates.state_abbr, IndexingStates.state_name)\
        .join(IndexingStates, IndexingCounties.state_fips == IndexingStates.fips_code)\
        .all()
    
    return jsonify([{
        'id': c.IndexingCounties.id,
        'name': c.IndexingCounties.county_name,
        'geo_id': c.IndexingCounties.geo_id,
        'state_fips': c.IndexingCounties.state_fips,
        'state_abbr': c.state_abbr,
        'state_name': c.state_name,
        'is_active': c.IndexingCounties.is_active,
        'is_enabled': c.IndexingCounties.is_enabled,
        'is_locked': c.IndexingCounties.is_locked,
        'notes': c.IndexingCounties.notes
    } for c in counties])
    # [GSI_END: cm_get_counties]

@county_mgmt_bp.route('/api/counties/add', methods=['POST'])
@login_required
def add_county():
    # [GSI_BLOCK: cm_add_county]
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.json
    try:
        new_county = IndexingCounties(
            county_name=data['county_name'],
            geo_id=data['geo_id'],
            state_fips=data['state_fips'],
            is_active=True,
            is_enabled=True,
            notes=data.get('notes', '')
        )
        db.session.add(new_county)
        db.session.commit()
        return jsonify({'success': True, 'message': 'County added successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: cm_add_county]

@county_mgmt_bp.route('/api/counties/edit', methods=['POST'])
@login_required
def edit_county():
    # [GSI_BLOCK: cm_edit_county]
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.json
    try:
        county = db.session.get(IndexingCounties, data['id'])
        if not county:
            return jsonify({'success': False, 'message': 'County not found'})
            
        county.county_name = data['county_name']
        county.geo_id = data['geo_id']
        county.state_fips = data['state_fips']
        county.notes = data.get('notes', '')
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'County updated successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: cm_edit_county]

@county_mgmt_bp.route('/api/counties/toggle', methods=['POST'])
@login_required
def toggle_county():
    # [GSI_BLOCK: cm_toggle_county]
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.json
    try:
        county = db.session.get(IndexingCounties, data['id'])
        if not county: return jsonify({'success': False, 'message': 'County not found'})
            
        if data['field'] == 'active':
            county.is_active = data['value']
        elif data['field'] == 'enabled':
            county.is_enabled = data['value']
        elif data['field'] == 'locked':
            county.is_locked = data['value']
            
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: cm_toggle_county]

@county_mgmt_bp.route('/api/counties/delete', methods=['POST'])
@login_required
def delete_county():
    # [GSI_BLOCK: cm_delete_county]
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    data = request.json
    try:
        county = db.session.get(IndexingCounties, data['id'])
        if not county: return jsonify({'success': False, 'message': 'County not found'})
            
        db.session.delete(county)
        db.session.commit()
        return jsonify({'success': True, 'message': 'County deleted'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: cm_delete_county]