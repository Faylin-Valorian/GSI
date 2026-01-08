import os
import json
import re
from flask import Blueprint, jsonify, request, url_for, current_app, session
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from extensions import db
from models import IndexingStates, IndexingCounties, CountyImages, Users
from utils import format_error

geo_bp = Blueprint('geospatial', __name__)

# --- HELPER: FOLDER MANAGEMENT ---
def ensure_folders(state_name, county_name=None):
    """Creates the standard directory structure including Images."""
    root = os.path.join(current_app.root_path, 'data')
    
    s_clean = secure_filename(state_name)
    state_path = os.path.join(root, s_clean)
    
    if not os.path.exists(state_path):
        os.makedirs(state_path)
        
    if county_name:
        c_clean = secure_filename(county_name)
        county_path = os.path.join(state_path, c_clean)
        if not os.path.exists(county_path):
            os.makedirs(county_path)
            
        # Ensure subdirectories exist
        edata = os.path.join(county_path, 'eData Files')
        keli = os.path.join(county_path, 'Keli Files')
        images = os.path.join(county_path, 'Images') 
        
        if not os.path.exists(edata): os.makedirs(edata)
        if not os.path.exists(keli): os.makedirs(keli)
        if not os.path.exists(images): os.makedirs(images)

# --- DEBUG TOGGLE (ADMIN) ---
@geo_bp.route('/api/admin/debug/toggle', methods=['POST'])
@login_required
def toggle_debug_mode():
    if current_user.role != 'admin': return jsonify({'success': False})
    current_mode = session.get('debug_mode', False)
    session['debug_mode'] = not current_mode
    return jsonify({'success': True, 'debug_mode': session['debug_mode']})

# --- DEBUG TOGGLE (USER - AUTH REQUIRED) ---
@geo_bp.route('/api/user/debug/auth-enable', methods=['POST'])
@login_required
def enable_debug_with_auth():
    # Verify the provided credentials belong to a real Admin
    username = request.json.get('username')
    password = request.json.get('password')
    
    admin_user = Users.query.filter_by(username=username).first()
    
    if admin_user and admin_user.role == 'admin' and admin_user.check_password(password):
        session['debug_mode'] = True
        return jsonify({'success': True})
    
    return jsonify({'success': False, 'message': 'Invalid Admin Credentials'})

@geo_bp.route('/api/user/debug/disable', methods=['POST'])
@login_required
def disable_user_debug():
    # Allow users to turn it off without auth
    session['debug_mode'] = False
    return jsonify({'success': True})

@geo_bp.route('/api/admin/debug/status', methods=['GET'])
@login_required
def get_debug_status():
    return jsonify({'debug_mode': session.get('debug_mode', False)})

# --- SEEDING ---
@geo_bp.route('/api/admin/seed', methods=['POST'])
@login_required
def seed_database():
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        with open(os.path.join(current_app.root_path, 'static/us-states.json')) as f: states = json.load(f)
        existing_s = {s.fips_code for s in IndexingStates.query.all()}
        new_s = [IndexingStates(state_name=f['properties']['NAME'], fips_code=f['properties']['STATE']) 
                 for f in states['features'] if f['properties']['STATE'] not in existing_s]
        if new_s: db.session.add_all(new_s)

        with open(os.path.join(current_app.root_path, 'static/us-counties.json')) as f: counties = json.load(f)
        existing_c = {c.geo_id for c in IndexingCounties.query.all()}
        new_c = [IndexingCounties(county_name=f['properties']['NAME'], geo_id=f['properties']['GEO_ID'], state_fips=f['properties']['STATE']) 
                 for f in counties['features'] if f['properties']['GEO_ID'] not in existing_c]
        if new_c: db.session.add_all(new_c)
        
        db.session.commit()
        return jsonify({'success': True, 'message': f"Seeded {len(new_s)} states, {len(new_c)} counties."})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)}), 500

# --- STATES ADMIN ---
@geo_bp.route('/api/admin/states', methods=['GET'])
@login_required
def get_all_states():
    return jsonify([{'id':s.id, 'name':s.state_name, 'fips':s.fips_code, 'is_enabled':s.is_enabled, 'is_locked':s.is_locked} for s in IndexingStates.query.order_by(IndexingStates.state_name).all()])

@geo_bp.route('/api/admin/state/<int:id>/toggle', methods=['POST'])
@login_required
def toggle_state(id):
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        s = db.session.get(IndexingStates, id)
        if s and not s.is_locked: 
            s.is_enabled = not s.is_enabled
            if s.is_enabled: ensure_folders(s.state_name)
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'State locked or not found'})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- COUNTIES ADMIN ---
@geo_bp.route('/api/admin/counties/<state_fips>', methods=['GET'])
@login_required
def get_counties_by_state(state_fips):
    c_list = IndexingCounties.query.filter_by(state_fips=state_fips).order_by(IndexingCounties.county_name).all()
    # Admin view sees RAW database values
    return jsonify([{'id':c.id, 'name':c.county_name, 'geo_id':c.geo_id, 'is_active':c.is_active, 'is_locked':c.is_locked, 'is_enabled':c.is_enabled} for c in c_list])

@geo_bp.route('/api/admin/county/<int:id>/toggle', methods=['POST'])
@login_required
def toggle_county_enabled(id):
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        c = db.session.get(IndexingCounties, id)
        if c and not c.is_locked: 
            c.is_enabled = not c.is_enabled
            if not c.is_enabled: c.is_active = False # Disable global active if hidden
            else:
                state = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
                if state: ensure_folders(state.state_name, c.county_name)
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- ADMIN TOGGLE GLOBAL ACTIVE ---
@geo_bp.route('/api/admin/county/<int:id>/set-global-active', methods=['POST'])
@login_required
def admin_set_global_active(id):
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        c = db.session.get(IndexingCounties, id)
        if c:
            c.is_active = not c.is_active
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False})
    except Exception as e: return jsonify({'success': False, 'message': format_error(e)})

# --- MAP LAYERS ---
@geo_bp.route('/api/layers/counties', methods=['GET'])
@login_required
def get_indexing_counties():
    # Fetch all enabled counties
    c_list = IndexingCounties.query.filter_by(is_enabled=True).all()
    
    # 1. My Working County
    my_working_id = current_user.current_working_county_id
    
    # 2. Get list of county IDs occupied by OTHER users
    # Query: Select current_working_county_id from Users where ID != Me
    others_working = db.session.query(Users.current_working_county_id)\
        .filter(Users.current_working_county_id.isnot(None))\
        .filter(Users.id != current_user.id)\
        .all()
    
    # Flatten tuple result to a Set for fast lookup
    occupied_ids = {row[0] for row in others_working}

    return jsonify([{
        'id': c.id, 
        'geo_id': c.geo_id, 
        'is_active': c.is_active,        # Global Status
        'name': c.county_name,
        'is_user_selected': (c.id == my_working_id), # Blue
        'is_occupied': (c.id in occupied_ids)        # Orange (Locked by others)
    } for c in c_list])

@geo_bp.route('/api/layers/states', methods=['GET'])
@login_required
def get_indexing_states():
    return jsonify([s.fips_code for s in IndexingStates.query.filter_by(is_enabled=True).all()])

# --- USER WORKING STATUS ---
@geo_bp.route('/api/user/set-working-county', methods=['POST'])
@login_required
def set_user_working_county():
    try:
        county_id = request.json.get('county_id')
        toggle_on = request.json.get('toggle_on') # True/False

        if toggle_on:
            # User wants to work on this county
            c = db.session.get(IndexingCounties, county_id)
            
            # Rule 1: Must be Active
            if not c or not c.is_active:
                return jsonify({'success': False, 'message': 'County is not active for indexing.'})
            
            # Rule 2: Must NOT be occupied by someone else
            conflict_user = Users.query.filter(
                Users.current_working_county_id == county_id, 
                Users.id != current_user.id
            ).first()
            
            if conflict_user:
                return jsonify({'success': False, 'message': f'Locked: User "{conflict_user.username}" is already working on this county.'})
            
            # Set user's focus
            current_user.current_working_county_id = c.id
        else:
            # User is stopping work
            if current_user.current_working_county_id == county_id:
                current_user.current_working_county_id = None

        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})

# --- POPUP DETAILS ---
@geo_bp.route('/api/county/<int:id>/details', methods=['GET'])
@login_required
def get_county_details(id):
    c = db.session.get(IndexingCounties, id)
    if not c: return jsonify({'success': False})
    
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    state_name = s.state_name if s else "Unknown"

    img = CountyImages.query.filter_by(county_id=id).first()
    image_url = url_for('static', filename=f'images/{img.image_path}') if img else None
    
    # Check if occupied by another user
    conflict_user = Users.query.filter(
        Users.current_working_county_id == c.id, 
        Users.id != current_user.id
    ).first()
    
    occupied_by = conflict_user.username if conflict_user else None
    
    return jsonify({
        'success': True,
        'id': c.id,
        'name': c.county_name,
        'state_name': state_name,
        'notes': c.notes or "",
        'image_url': image_url,
        'is_active': c.is_active, 
        'is_user_selected': (current_user.current_working_county_id == c.id),
        'is_occupied': bool(occupied_by),  # New Flag
        'occupied_by': occupied_by         # Username of owner
    })

@geo_bp.route('/api/county/<int:id>/save-notes', methods=['POST'])
@login_required
def save_county_notes(id):
    try:
        c = db.session.get(IndexingCounties, id)
        # Only allow edits if user is working on it
        if not c or current_user.current_working_county_id != c.id: 
            return jsonify({'success': False, 'message': 'You must be working on this county to edit notes.'})
        
        c.notes = request.json.get('notes')
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})

@geo_bp.route('/api/county/<int:id>/upload-image', methods=['POST'])
@login_required
def upload_county_image(id):
    try:
        # Check permissions
        if current_user.current_working_county_id != id:
             return jsonify({'success': False, 'message': 'You must be working on this county to upload images.'})

        if 'file' not in request.files or request.files['file'].filename == '': return jsonify({'success': False})
        f = request.files['file']
        fname = secure_filename(f"county_{id}_{f.filename}")
        f.save(os.path.join(current_app.config['UPLOAD_FOLDER'], fname))
        
        img = CountyImages.query.filter_by(county_id=id).first()
        if not img: img = CountyImages(county_id=id, image_path=fname); db.session.add(img)
        else: img.image_path = fname
        db.session.commit()
        return jsonify({'success': True, 'image_url': url_for('static', filename=f'images/{fname}')})
    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})

# --- SQL VIEWER ---
@geo_bp.route('/api/admin/view-code', methods=['POST'])
@login_required
def view_tool_code():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    tool_key = request.json.get('tool')
    file_map = {
        'drive': 'blueprints/OpenDriveConnection.py',
        'compat': 'blueprints/DatabaseCompatibility.py',
        'procedures': 'blueprints/SetupDatabaseProcedures.py',
        'edata': 'blueprints/SetupEDataTable.py',
        'keli': 'blueprints/SetupKeliTables.py',
        'seed': 'blueprints/geospatial.py',
        'unindexed': 'blueprints/UnindexedImages.py' # <--- ADDED
    }
    filename = file_map.get(tool_key)
    if not filename: return jsonify({'success': False, 'message': 'Unknown Tool'})
    try:
        file_path = os.path.join(current_app.root_path, filename)
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        extracted_sql = []
        big_blocks = re.findall(r'"""(.*?)"""', content, re.DOTALL)
        for block in big_blocks:
            if any(k in block.upper() for k in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'BULK INSERT']):
                extracted_sql.append(block.strip())
        lines = content.split('\n')
        for line in lines:
            clean = line.strip()
            # Added regex matching for 'sql =' and 'text(' lines common in Flask-SQLAlchemy
            if (clean.startswith('sql =') or clean.startswith('cmd =') or 'text(' in clean) and any(k in clean.upper() for k in ['ALTER DATABASE', 'SELECT', 'NET USE']):
                clean = clean.replace('sql = f"', '').replace('sql = "', '').replace('"', '').replace('cmd = f\'', '').replace('cmd = \'', '').replace('\'', '').replace('text(', '').replace(')', '')
                extracted_sql.append(clean.strip())
        final_output = "\n\n--------------------------------------\n\n".join(extracted_sql) if extracted_sql else "-- No explicit SQL/Command blocks found in this file --"
        return jsonify({'success': True, 'filename': filename, 'code': final_output})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})