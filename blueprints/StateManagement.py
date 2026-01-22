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
    return jsonify([{'id': s.id, 'name': s.state_name, 'fips': s.fips_code, 'status': s.is_enabled} for s in states])

@state_mgmt_bp.route('/api/admin/states/toggle', methods=['POST'])
@login_required
def toggle_state():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    s = db.session.get(IndexingStates, data.get('id'))
    if s:
        s.is_enabled = bool(data.get('status'))
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False})

@state_mgmt_bp.route('/api/admin/seed-db', methods=['POST'])
@login_required
def seed_database():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        states_path = os.path.join(current_app.root_path, 'static', 'us-states.json')
        counties_path = os.path.join(current_app.root_path, 'static', 'us-counties.json')
        
        if not os.path.exists(states_path) or not os.path.exists(counties_path):
            return jsonify({'success': False, 'message': 'JSON source files missing.'})

        # 1. States (GeoJSON)
        with open(states_path, 'r') as f:
            data = json.load(f)
            # Handle FeatureCollection vs List
            items = data.get('features', []) if isinstance(data, dict) and data.get('type') == 'FeatureCollection' else data

        added_states = 0
        for item in items:
            props = item.get('properties', item) # Handle GeoJSON or flat dict
            
            # Extract ID and Name (GeoJSON uses 'STATE', flat uses 'id')
            fips = props.get('STATE') or props.get('id')
            name = props.get('NAME') or props.get('name')
            
            if not fips or not name: continue

            if not IndexingStates.query.filter_by(fips_code=fips).first():
                db.session.add(IndexingStates(state_name=name, fips_code=fips, is_enabled=False))
                added_states += 1
        db.session.commit()

        # 2. Counties (GeoJSON)
        with open(counties_path, 'r') as f:
            data = json.load(f)
            items = data.get('features', []) if isinstance(data, dict) and data.get('type') == 'FeatureCollection' else data
            
        added_counties = 0
        state_map = {s.fips_code: s.fips_code for s in IndexingStates.query.all()}
        
        for item in items:
            props = item.get('properties', item)
            
            # GeoJSON: STATE="01", COUNTY="001" -> FIPS="01001"
            # Flat: id="01001"
            s_fips = props.get('STATE')
            c_code = props.get('COUNTY')
            
            if s_fips and c_code:
                full_fips = s_fips + c_code
            else:
                full_fips = props.get('id')
                if full_fips and len(full_fips) == 5:
                    s_fips = full_fips[:2]
                else:
                    continue

            name = props.get('NAME') or props.get('name')

            if s_fips in state_map and name:
                if not IndexingCounties.query.filter_by(county_fips=full_fips).first():
                    db.session.add(IndexingCounties(
                        county_name=name, 
                        county_fips=full_fips, 
                        state_fips=s_fips, 
                        is_active=False, 
                        is_enabled=False
                    ))
                    added_counties += 1
        db.session.commit()

        return jsonify({'success': True, 'message': f'Seeding Complete. +{added_states} States, +{added_counties} Counties.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f"Seeding Error: {str(e)}"})