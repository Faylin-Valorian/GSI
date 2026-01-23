import os
from flask import Blueprint, jsonify, request, url_for, current_app
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from extensions import db
from models import IndexingCounties, CountyImages, Users, IndexingStates
from utils import format_error

workflow_bp = Blueprint('workflow', __name__)

@workflow_bp.route('/api/county/<int:id>/details', methods=['GET'])
@login_required
def get_county_details(id):
    c = db.session.get(IndexingCounties, id)
    if not c: return jsonify({'success': False})
    
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    state_name = s.state_name if s else "Unknown"
    
    img = CountyImages.query.filter_by(county_id=id).first()
    img_url = url_for('static', filename=f'images/{img.image_path}') if img else None
    
    occupier = Users.query.filter(Users.current_working_county_id == id, Users.id != current_user.id).first()
    
    return jsonify({
        'success': True,
        'id': c.id, 'name': c.county_name, 'state_name': state_name,
        'notes': c.notes, 'image_url': img_url,
        'is_mine': current_user.current_working_county_id == id,
        'occupied_by': occupier.username if occupier else None
    })

@workflow_bp.route('/api/user/set-working', methods=['POST'])
@login_required
def set_working():
    try:
        cid = request.json.get('county_id')
        active = request.json.get('active')
        
        if active:
            # Check lock
            conflict = Users.query.filter(Users.current_working_county_id == cid, Users.id != current_user.id).first()
            if conflict: return jsonify({'success': False, 'message': f'Locked by {conflict.username}'})
            current_user.current_working_county_id = cid
        else:
            if current_user.current_working_county_id == cid:
                current_user.current_working_county_id = None
                
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})

@workflow_bp.route('/api/county/<int:id>/save-notes', methods=['POST'])
@login_required
def save_notes(id):
    c = db.session.get(IndexingCounties, id)
    if c:
        c.notes = request.json.get('notes')
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False})

@workflow_bp.route('/api/county/<int:id>/upload', methods=['POST'])
@login_required
def upload_image(id):
    if 'file' not in request.files: return jsonify({'success': False})
    f = request.files['file']
    fname = secure_filename(f"county_{id}_{f.filename}")
    f.save(os.path.join(current_app.config['UPLOAD_FOLDER'], fname))
    
    img = CountyImages.query.filter_by(county_id=id).first()
    if not img: db.session.add(CountyImages(county_id=id, image_path=fname))
    else: img.image_path = fname
    db.session.commit()
    return jsonify({'success': True, 'url': url_for('static', filename=f'images/{fname}')})