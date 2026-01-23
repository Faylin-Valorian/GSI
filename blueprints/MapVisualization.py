import os
import json
from flask import Blueprint, jsonify, current_app
from flask_login import login_required, current_user
from extensions import db
from models import IndexingStates, IndexingCounties, Users

map_viz_bp = Blueprint('map_viz', __name__)

@map_viz_bp.route('/api/map/states', methods=['GET'])
@login_required
def get_state_shapes():
    states = IndexingStates.query.filter_by(is_enabled=True).all()
    enabled_fips = {s.fips_code for s in states}
    
    path = os.path.join(current_app.root_path, 'static', 'us-states.json')
    if not os.path.exists(path): return jsonify({"type": "FeatureCollection", "features": []})

    with open(path, 'r') as f: data = json.load(f)
    
    filtered = []
    features = data.get('features', []) if isinstance(data, dict) else data
    
    for f in features:
        props = f.get('properties', {})
        # Robust ID check
        fips = props.get('STATE') or props.get('id')
        if fips in enabled_fips:
            filtered.append(f)
            
    return jsonify({"type": "FeatureCollection", "features": filtered})

@map_viz_bp.route('/api/map/counties', methods=['GET'])
@login_required
def get_county_shapes():
    # 1. Fetch Enabled Data
    states = IndexingStates.query.filter_by(is_enabled=True).all()
    enabled_state_fips = {s.fips_code for s in states}
    
    counties = IndexingCounties.query.filter_by(is_enabled=True).all()
    
    # 2. Get Occupied Status
    others_working = db.session.query(Users.current_working_county_id).filter(
        Users.current_working_county_id.isnot(None),
        Users.id != current_user.id
    ).all()
    occupied_ids = {r[0] for r in others_working}

    # 3. Build fast lookup map
    # Key: geo_id (e.g., '01001'), Value: Metadata
    meta_map = {}
    for c in counties:
        if c.state_fips in enabled_state_fips:
            meta_map[c.geo_id] = {
                'db_id': c.id,
                'is_active': c.is_active,
                'is_occupied': c.id in occupied_ids,
                'is_mine': c.id == current_user.current_working_county_id,
                'notes': c.notes
            }

    # 4. Filter GeoJSON
    path = os.path.join(current_app.root_path, 'static', 'us-counties.json')
    if not os.path.exists(path): return jsonify({"type": "FeatureCollection", "features": []})

    with open(path, 'r') as f: data = json.load(f)
    
    filtered = []
    features = data.get('features', []) if isinstance(data, dict) else data
    
    for f in features:
        props = f.get('properties', {})
        
        # Robust ID Construction (Padding is crucial)
        s = props.get('STATE', '')
        c = props.get('COUNTY', '')
        geo_id = props.get('id') or props.get('GEO_ID')
        
        check_ids = []
        if s and c: check_ids.append(s + c)
        if geo_id: check_ids.append(geo_id)
        
        # Check against DB
        found_meta = None
        for cid in check_ids:
            if cid in meta_map:
                found_meta = meta_map[cid]
                break
        
        if found_meta:
            props.update(found_meta) # Inject DB data into GeoJSON
            filtered.append(f)

    return jsonify({"type": "FeatureCollection", "features": filtered})