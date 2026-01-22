import os
import json
from flask import Blueprint, jsonify, current_app, request
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates, IndexingCounties
from utils import format_error

state_mgmt_bp = Blueprint('state_mgmt', __name__)

@state_mgmt_bp.route('/api/admin/states/list', methods=['GET'])
@login_required
def list_states():
    if current_user.role != 'admin': return jsonify([])
    
    states = IndexingStates.query.order_by(IndexingStates.state_name).all()
    return jsonify([{
        'id': s.id,
        'name': s.state_name,
        'fips': s.fips_code,
        'status': s.is_enabled
    } for s in states])

@state_mgmt_bp.route('/api/admin/states/toggle', methods=['POST'])
@login_required
def toggle_state():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    try:
        s = db.session.get(IndexingStates, data.get('id'))
        if s:
            s.is_enabled = bool(data.get('status'))
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'State not found'})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})

@state_mgmt_bp.route('/api/admin/seed-db', methods=['POST'])
@login_required
def seed_database():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    try:
        # 1. Load States
        states_path = os.path.join(current_app.root_path, 'static', 'us-states.json')
        counties_path = os.path.join(current_app.root_path, 'static', 'us-counties.json')
        
        if not os.path.exists(states_path) or not os.path.exists(counties_path):
            return jsonify({'success': False, 'message': 'JSON source files missing in static folder.'})

        with open(states_path, 'r') as f:
            states_data = json.load(f)
        
        added_states = 0
        for item in states_data:
            if not IndexingStates.query.filter_by(fips_code=item['id']).first():
                db.session.add(IndexingStates(state_name=item['name'], fips_code=item['id'], is_enabled=False))
                added_states += 1
        db.session.commit()

        # 2. Load Counties
        with open(counties_path, 'r') as f:
            counties_data = json.load(f)
            
        added_counties = 0
        # Cache states for performance
        state_map = {s.fips_code: s.fips_code for s in IndexingStates.query.all()}
        
        for item in counties_data:
            s_fips = item['id'][:2]
            if s_fips in state_map:
                if not IndexingCounties.query.filter_by(county_fips=item['id']).first():
                    db.session.add(IndexingCounties(
                        county_name=item['name'], 
                        county_fips=item['id'], 
                        state_fips=s_fips,
                        is_active=False,
                        is_enabled=False
                    ))
                    added_counties += 1
        db.session.commit()

        return jsonify({'success': True, 'message': f'Seeding Complete. Added {added_states} States, {added_counties} Counties.'})

    except Exception as e:
        return jsonify({'success': False, 'message': f"Seeding Error: {str(e)}"})