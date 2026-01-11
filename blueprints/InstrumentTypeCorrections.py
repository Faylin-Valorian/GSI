import os
import json
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties, IndexingStates
from werkzeug.utils import secure_filename

inst_type_bp = Blueprint('instrument_type_corrections', __name__)

def get_tables(county_name):
    return {
        'corrections': f"{county_name}_Instrument_Type_Corrections",
        'inst_types': f"{county_name}_keli_InstTypes_Externals"
    }

@inst_type_bp.route('/api/tools/inst-corrections/init', methods=['POST'])
@login_required
def init_tool():
    """Seeds the corrections table with NEW unique values only."""
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
            OriginalCol03Varchar NVARCHAR(MAX),
            CorrectedCol03Varchar NVARCHAR(MAX)
        )
        """
        db.session.execute(text(sql_create))
        
        # 2. Seed Missing Values
        sql_seed = f"""
        INSERT INTO {tables['corrections']} (OriginalCol03Varchar, CorrectedCol03Varchar)
        SELECT DISTINCT col03varchar, NULL
        FROM GenericDataImport
        WHERE col03varchar IS NOT NULL 
          AND col03varchar NOT IN (SELECT OriginalCol03Varchar FROM {tables['corrections']})
        """
        db.session.execute(text(sql_seed))
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@inst_type_bp.route('/api/tools/inst-corrections/list', methods=['POST'])
@login_required
def get_correction_list():
    """Returns records, optionally filtering out completed ones."""
    county_id = request.json.get('county_id')
    hide_completed = request.json.get('hide_completed', True) # Default True
    
    c = db.session.get(IndexingCounties, county_id)
    tables = get_tables(c.county_name)
    
    sql = f"SELECT id, OriginalCol03Varchar, CorrectedCol03Varchar FROM {tables['corrections']}"
    
    if hide_completed:
        sql += " WHERE CorrectedCol03Varchar IS NULL OR CorrectedCol03Varchar = ''"
    
    sql += " ORDER BY OriginalCol03Varchar"
    
    results = db.session.execute(text(sql)).fetchall()
    
    return jsonify({
        'success': True, 
        'records': [{
            'id': r.id, 
            'value': r.OriginalCol03Varchar,
            'corrected': r.CorrectedCol03Varchar 
        } for r in results]
    })

@inst_type_bp.route('/api/tools/inst-corrections/images', methods=['POST'])
@login_required
def get_images_for_record():
    """Fetches images for a specific instrument type grouping."""
    county_id = request.json.get('county_id')
    original_val = request.json.get('value')
    
    c = db.session.get(IndexingCounties, county_id)
    
    # 1. Find a Header row
    sql_sample = """
    SELECT TOP 1 OriginalValue 
    FROM GenericDataImport 
    WHERE col03varchar = :val AND fn LIKE '%header%'
    """
    sample = db.session.execute(text(sql_sample), {'val': original_val}).fetchone()
    
    images = []
    if sample:
        # 2. Get images matching that key
        sql_imgs = """
        SELECT stech_image_path 
        FROM GenericDataImport 
        WHERE fn LIKE '%image%' AND keyOriginalValue = :key
        ORDER BY fn
        """
        imgs = db.session.execute(text(sql_imgs), {'key': sample.OriginalValue}).fetchall()
        
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        base = f"/static/data/{secure_filename(s.state_name)}/{secure_filename(c.county_name)}/Images/"
        images = [base + i.stech_image_path for i in imgs if i.stech_image_path]

    return jsonify({'success': True, 'images': images})

@inst_type_bp.route('/api/tools/inst-corrections/search', methods=['POST'])
@login_required
def search_types():
    data = request.json
    term = data.get('term', '')
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    tables = get_tables(c.county_name)
    
    # Search InstTypeName
    sql = f"SELECT TOP 50 InstTypeName FROM {tables['inst_types']} WHERE InstTypeName LIKE :term"
    results = db.session.execute(text(sql), {'term': f'%{term}%'}).fetchall()
    
    return jsonify([r[0] for r in results])

@inst_type_bp.route('/api/tools/inst-corrections/save', methods=['POST'])
@login_required
def save_correction():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data['county_id'])
    tables = get_tables(c.county_name)
    
    try:
        # 1. Update Lookup
        sql_lookup = f"UPDATE {tables['corrections']} SET CorrectedCol03Varchar = :new WHERE id = :id"
        db.session.execute(text(sql_lookup), {'new': data['corrected'], 'id': data['id']})
        
        # 2. Update Data
        sql_data = "UPDATE GenericDataImport SET col03varchar = :new WHERE col03varchar = :old"
        db.session.execute(text(sql_data), {'new': data['corrected'], 'old': data['original']})
        
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})