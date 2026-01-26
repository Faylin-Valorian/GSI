import os
import json
import io
import urllib.parse
from flask import Blueprint, request, jsonify, current_app, send_file
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties, IndexingStates
from werkzeug.utils import secure_filename

# Try to import PIL for image serving
try:
    from PIL import Image
except ImportError:
    Image = None

additions_bp = Blueprint('additions_corrections', __name__)

def get_tables(county_name):
    return {
        'corrections': f"{county_name}_Additions_Corrections",
        'additions': f"{county_name}_keli_additions"
    }

@additions_bp.route('/api/tools/additions-corrections/init', methods=['POST'])
@login_required
def init_tool():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    county_id = request.json.get('county_id')
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    
    tables = get_tables(c.county_name)
    
    try:
        # 1. Create Table
        sql_create = f"""
        IF OBJECT_ID('{tables['corrections']}', 'U') IS NULL
        CREATE TABLE {tables['corrections']} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            OriginalCol05Varchar NVARCHAR(MAX),
            CorrectedCol05Varchar NVARCHAR(MAX)
        )
        """
        db.session.execute(text(sql_create))
        
        # 2. Auto-Import / Merge
        sql_merge = f"""
        UPDATE g
        SET g.col05varchar = c.CorrectedCol05Varchar
        FROM GenericDataImport g
        INNER JOIN {tables['corrections']} c ON g.col05varchar = c.OriginalCol05Varchar
        WHERE g.fn LIKE '%legal%'
          AND c.CorrectedCol05Varchar IS NOT NULL 
          AND c.CorrectedCol05Varchar <> ''
        """
        db.session.execute(text(sql_merge))
        
        # 3. Seed Missing Values from Legals
        sql_seed = f"""
        INSERT INTO {tables['corrections']} (OriginalCol05Varchar, CorrectedCol05Varchar)
        SELECT DISTINCT col05varchar, NULL
        FROM GenericDataImport
        WHERE fn LIKE '%legal%' 
          AND col05varchar IS NOT NULL 
          AND col05varchar <> ''
          AND col05varchar NOT IN (SELECT OriginalCol05Varchar FROM {tables['corrections']})
        """
        db.session.execute(text(sql_seed))
        
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})

@additions_bp.route('/api/tools/additions-corrections/list', methods=['POST'])
@login_required
def get_correction_list():
    county_id = request.json.get('county_id')
    hide_completed = request.json.get('hide_completed', True)
    
    c = db.session.get(IndexingCounties, county_id)
    tables = get_tables(c.county_name)
    
    sql = f"SELECT id, OriginalCol05Varchar, CorrectedCol05Varchar FROM {tables['corrections']}"
    if hide_completed:
        sql += " WHERE CorrectedCol05Varchar IS NULL OR CorrectedCol05Varchar = ''"
    sql += " ORDER BY OriginalCol05Varchar"
    
    results = db.session.execute(text(sql)).fetchall()
    
    return jsonify({
        'success': True, 
        'records': [{
            'id': r.id, 
            'value': r.OriginalCol05Varchar,
            'corrected': r.CorrectedCol05Varchar 
        } for r in results]
    })

@additions_bp.route('/api/tools/additions-corrections/images', methods=['POST'])
@login_required
def get_images_for_record():
    county_id = request.json.get('county_id')
    original_val = request.json.get('value')
    relative_base_path = request.json.get('base_path') 
    
    c = db.session.get(IndexingCounties, county_id)
    
    # 1. Find a Legal row using this addition name to get the key link
    sql_link = "SELECT TOP 1 keyOriginalValue FROM GenericDataImport WHERE col05varchar = :val AND fn LIKE '%legal%'"
    link_res = db.session.execute(text(sql_link), {'val': original_val}).fetchone()
    
    header_text = "No Document Found"
    images = []

    if link_res and link_res.keyOriginalValue:
        key_val = link_res.keyOriginalValue
        header_text = f"Linked Header Key: {key_val}"

        # 2. Get images linked to the Header via the key
        sql_imgs = "SELECT col03varchar FROM GenericDataImport WHERE fn LIKE '%image%' AND keyOriginalValue = :key ORDER BY fn"
        imgs = db.session.execute(text(sql_imgs), {'key': key_val}).fetchall()
        
        # 3. Resolve Path
        if relative_base_path:
             if not os.path.isabs(relative_base_path):
                 abs_base = os.path.join(current_app.root_path, relative_base_path)
             else:
                 abs_base = relative_base_path

             for i in imgs:
                 filename = i.col03varchar
                 if filename:
                     full_disk_path = os.path.join(abs_base, filename)
                     safe_path = urllib.parse.quote(full_disk_path)
                     images.append({
                         'src': f"/api/tools/inst-corrections/view-image?path={safe_path}", # Reusing existing image viewer endpoint
                         'path': full_disk_path,
                         'name': filename
                     })

    return jsonify({
        'success': True, 
        'images': images,
        'header_text': header_text
    })

@additions_bp.route('/api/tools/additions-corrections/search', methods=['POST'])
@login_required
def search_additions():
    data = request.json
    term = data.get('term', '')
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify([])

    tables = get_tables(c.county_name)
    target_table = tables['additions'] 
    
    try:
        # Search name or comments (description)
        # Select: Name, Description (comments), Active Status (active)
        sql = f"""
        SELECT TOP 50 name, comments, active 
        FROM [{target_table}] 
        WHERE name LIKE :term OR comments LIKE :term
        """
        
        results = db.session.execute(text(sql), {'term': f'%{term}%'}).fetchall()
        
        out = []
        for r in results:
            out.append({
                'name': r.name, 
                'desc': r.comments if r.comments else '', 
                'active': True if r.active == 1 else False
            })
            
        return jsonify(out)

    except Exception as e:
        return jsonify([])

@additions_bp.route('/api/tools/additions-corrections/save', methods=['POST'])
@login_required
def save_correction():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data['county_id'])
    tables = get_tables(c.county_name)
    try:
        # Update Corrections Table
        db.session.execute(text(f"UPDATE {tables['corrections']} SET CorrectedCol05Varchar = :new WHERE OriginalCol05Varchar = :old"), {'new': data['corrected'], 'old': data['original']})
        
        # Update GenericDataImport (Legals only)
        db.session.execute(text("UPDATE GenericDataImport SET col05varchar = :new WHERE col05varchar = :old AND fn LIKE '%legal%'"), {'new': data['corrected'], 'old': data['original']})
        
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})