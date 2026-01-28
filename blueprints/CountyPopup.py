import os
import time
from flask import Blueprint, jsonify, request, url_for, current_app, render_template
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from extensions import db
from models import IndexingCounties, CountyImages, Users, IndexingStates
from utils import format_error

workflow_bp = Blueprint('workflow', __name__)

# [GSI_BLOCK: workflow_popup]
@workflow_bp.route('/api/county/<int:id>/popup', methods=['GET'])
@login_required
def get_county_popup(id):
    c = db.session.get(IndexingCounties, id)
    if not c: return "Error: County not found", 404
    
    s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    state_name = s.state_name if s else "Unknown"
    
    img = CountyImages.query.filter_by(county_id=id).first()
    img_url = url_for('static', filename=f'images/{img.image_path}') if img else None
    
    occupier = Users.query.filter(Users.current_working_county_id == id, Users.id != current_user.id).first()
    
    # Render the template on the server
    return render_template('components/popups/CountyPopup.html',
        id=c.id,
        name=c.county_name,
        state_name=state_name,
        notes=c.notes or "",
        image_url=img_url,
        is_active=c.is_active,
        is_split_job=c.is_split_job, # [NEW] Pass split status to template
        is_mine=(current_user.current_working_county_id == id),
        occupied_by=occupier.username if occupier else None,
        time=int(time.time()) # For cache busting images
    )
# [GSI_END: workflow_popup]

# [GSI_BLOCK: workflow_toggle_work]
@workflow_bp.route('/api/county/toggle-work', methods=['POST'])
@login_required
def toggle_work():
    try:
        cid = request.json.get('county_id')
        status = request.json.get('status')
        
        if status:
            # Check lock
            conflict = Users.query.filter(Users.current_working_county_id == cid, Users.id != current_user.id).first()
            if conflict: return jsonify({'success': False, 'message': f'Locked by {conflict.username}'})
            current_user.current_working_county_id = cid
        else:
            if current_user.current_working_county_id == int(cid):
                current_user.current_working_county_id = None
                
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})
# [GSI_END: workflow_toggle_work]

# [GSI_BLOCK: workflow_toggle_split]
@workflow_bp.route('/api/county/toggle-split', methods=['POST'])
@login_required
def toggle_split():
    # Only Admin or authorized users should change job configuration
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'})
    try:
        cid = request.json.get('county_id')
        status = request.json.get('status')
        
        c = db.session.get(IndexingCounties, cid)
        if c:
            c.is_split_job = status
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'County not found'})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})
# [GSI_END: workflow_toggle_split]

# [GSI_BLOCK: workflow_save_notes]
@workflow_bp.route('/api/county/save-notes', methods=['POST'])
@login_required
def save_notes():
    id = request.json.get('county_id')
    c = db.session.get(IndexingCounties, id)
    if c:
        c.notes = request.json.get('notes')
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False})
# [GSI_END: workflow_save_notes]

# [GSI_BLOCK: workflow_upload]
@workflow_bp.route('/api/county/upload-image', methods=['POST'])
@login_required
def upload_image():
    if 'file' not in request.files: return jsonify({'success': False})
    
    id = request.form.get('county_id')
    f = request.files['file']
    
    if not id: return jsonify({'success': False, 'message': 'No county ID provided'})

    fname = secure_filename(f"county_{id}_{f.filename}")
    f.save(os.path.join(current_app.config['UPLOAD_FOLDER'], fname))
    
    img = CountyImages.query.filter_by(county_id=id).first()
    if not img: db.session.add(CountyImages(county_id=id, image_path=fname))
    else: img.image_path = fname
    db.session.commit()
    return jsonify({'success': True, 'url': url_for('static', filename=f'images/{fname}')})
# [GSI_END: workflow_upload]