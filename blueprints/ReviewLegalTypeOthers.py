import os
import json
import urllib.parse
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text, inspect
from extensions import db
from models import IndexingCounties, IndexingStates

review_legal_bp = Blueprint('review_legal_others', __name__)

def get_tables(county_name):
    return {
        'data': 'GenericDataImport',
        'tr': f"{county_name}_keli_township_ranges",
        'adds': f"{county_name}_keli_additions"
    }

@review_legal_bp.route('/api/tools/legal-others/init', methods=['POST'])
@login_required
def init_tool():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    county_id = request.json.get('county_id')
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    
    try:
        # Safety Check for Column
        inspector = inspect(db.engine)
        columns = [col['name'] for col in inspector.get_columns('GenericDataImport')]
        if 'legal_type' not in columns:
            return jsonify({'success': False, 'message': "Column 'legal_type' missing. Run Setup eData Table."})

        # FETCH: We need keyOriginalValue for the Clipboard and Image Linking
        sql = """
        SELECT id, keyOriginalValue, col02varchar, col03varchar, col04varchar, 
               col05varchar, col06varchar, col07varchar, col08varchar
        FROM GenericDataImport
        WHERE fn LIKE '%legal%' 
          AND (legal_type = 'Other' OR legal_type = 'O')
        ORDER BY id
        """
        results = db.session.execute(text(sql)).fetchall()
        
        records = []
        for r in results:
            records.append({
                'id': r.id,
                'desc': r.keyOriginalValue, # KEY VALUE for Clipboard/Display
                'fields': {
                    'col02': r.col02varchar,
                    'col03': r.col03varchar,
                    'col04': r.col04varchar,
                    'col05': r.col05varchar,
                    'col06': r.col06varchar,
                    'col07': r.col07varchar,
                    'col08': r.col08varchar
                }
            })
            
        return jsonify({'success': True, 'records': records})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@review_legal_bp.route('/api/tools/legal-others/images', methods=['POST'])
@login_required
def get_images():
    try:
        record_id = request.json.get('record_id')
        county_id = request.json.get('county_id')
        
        c = db.session.get(IndexingCounties, county_id)
        
        # 1. Get the Key (keyOriginalValue) from the selected record
        sql_key = "SELECT keyOriginalValue FROM GenericDataImport WHERE id = :id"
        key_res = db.session.execute(text(sql_key), {'id': record_id}).fetchone()
        
        if not key_res or not key_res[0]: 
            return jsonify({'success': False, 'images': [], 'message': 'No Key Found'})
        
        key_val = key_res[0]
        
        # 2. Find Images sharing that Key (fn LIKE '%image%')
        # We grab col03varchar which usually holds the Filename
        sql_imgs = """
        SELECT col03varchar 
        FROM GenericDataImport 
        WHERE fn LIKE '%image%' AND keyOriginalValue = :key 
        ORDER BY fn
        """
        imgs = db.session.execute(text(sql_imgs), {'key': key_val}).fetchall()
        
        # 3. Construct Path based on Folder Structure: data/{State}/{County}/Images/{Filename}
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        state_name = s.state_name
        base_path = os.path.join(current_app.root_path, 'data', state_name, c.county_name, 'Images')
        
        images = []
        for i in imgs:
            filename = i.col03varchar
            if filename:
                full_path = os.path.join(base_path, filename)
                # We send the path to the 'view-image' endpoint just like Instrument Corrections
                safe_path = urllib.parse.quote(full_path)
                images.append({
                    'src': f"/api/tools/inst-corrections/view-image?path={safe_path}",
                    'name': filename
                })

        return jsonify({'success': True, 'images': images})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@review_legal_bp.route('/api/tools/legal-others/search-tr', methods=['POST'])
@login_required
def search_tr():
    try:
        data = request.json
        c = db.session.get(IndexingCounties, data.get('county_id'))
        tbl = get_tables(c.county_name)['tr']
        col = 'Township' if data.get('mode') == 'township' else 'Range'
        sql = f"SELECT DISTINCT {col} FROM {tbl} WHERE {col} LIKE :term AND Active = 1 ORDER BY {col}"
        res = db.session.execute(text(sql), {'term': f"%{data.get('term','')}%"}).fetchall()
        return jsonify([r[0] for r in res])
    except: return jsonify([])

@review_legal_bp.route('/api/tools/legal-others/search-adds', methods=['POST'])
@login_required
def search_adds():
    try:
        data = request.json
        c = db.session.get(IndexingCounties, data.get('county_id'))
        tbl = get_tables(c.county_name)['adds']
        sql = f"SELECT Name FROM {tbl} WHERE Name LIKE :term AND Active = 1 ORDER BY Name"
        res = db.session.execute(text(sql), {'term': f"%{data.get('term','')}%"}).fetchall()
        return jsonify([r[0] for r in res])
    except: return jsonify([])

@review_legal_bp.route('/api/tools/legal-others/save', methods=['POST'])
@login_required
def save_record():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    try:
        # Note: ValidationStatus Removed
        sql = """
        UPDATE GenericDataImport
        SET col02varchar = :c2, col03varchar = :c3, col04varchar = :c4,
            col05varchar = :c5, col06varchar = :c6, col07varchar = :c7,
            col08varchar = :c8
        WHERE id = :id
        """
        db.session.execute(text(sql), {
            'c2': data.get('col02'), 'c3': data.get('col03'), 'c4': data.get('col04'),
            'c5': data.get('col05'), 'c6': data.get('col06'), 'c7': data.get('col07'),
            'c8': data.get('col08'), 'id': data.get('id')
        })
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})