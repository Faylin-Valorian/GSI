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

inst_type_bp = Blueprint('instrument_type_corrections', __name__)

def get_tables(county_name):
    return {
        'corrections': f"{county_name}_Instrument_Type_Corrections",
        'inst_types': f"{county_name}_keli_instrument_types"
    }

@inst_type_bp.route('/api/tools/inst-corrections/init', methods=['POST'])
@login_required
def init_tool():
    """
    1. Creates Corrections table if missing.
    2. Auto-Merges: Updates GenericDataImport headers using instTypeOriginal.
    3. Seeds: Finds new unique header values in instTypeOriginal and adds them.
    """
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
        
        # 2. Auto-Import / Merge
        # Match using instTypeOriginal (The source of truth for the 'Original' value)
        sql_merge = f"""
        UPDATE g
        SET g.col03varchar = c.CorrectedCol03Varchar
        FROM GenericDataImport g
        INNER JOIN {tables['corrections']} c ON g.instTypeOriginal = c.OriginalCol03Varchar
        WHERE g.fn LIKE '%header%'
          AND c.CorrectedCol03Varchar IS NOT NULL 
          AND c.CorrectedCol03Varchar <> ''
        """
        db.session.execute(text(sql_merge))
        
        # 3. Seed Missing Values
        # Pull from instTypeOriginal to populate the Corrections table
        sql_seed = f"""
        INSERT INTO {tables['corrections']} (OriginalCol03Varchar, CorrectedCol03Varchar)
        SELECT DISTINCT instTypeOriginal, NULL
        FROM GenericDataImport
        WHERE fn LIKE '%header%' 
          AND instTypeOriginal IS NOT NULL 
          AND instTypeOriginal NOT IN (SELECT OriginalCol03Varchar FROM {tables['corrections']})
        """
        db.session.execute(text(sql_seed))
        
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})

@inst_type_bp.route('/api/tools/inst-corrections/list', methods=['POST'])
@login_required
def get_correction_list():
    """Returns records, optionally filtering out completed ones."""
    county_id = request.json.get('county_id')
    hide_completed = request.json.get('hide_completed', True)
    
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
    """Fetches images and header info based on the ORIGINAL Instrument Type."""
    county_id = request.json.get('county_id')
    original_val = request.json.get('value') # This is the Original Value
    relative_base_path = request.json.get('base_path') 
    
    c = db.session.get(IndexingCounties, county_id)
    
    # 1. Find a Header row using instTypeOriginal
    # This allows us to find the record even if col03varchar has been changed
    sql_sample = """
    SELECT TOP 1 OriginalValue 
    FROM GenericDataImport 
    WHERE instTypeOriginal = :val AND fn LIKE '%header%'
    """
    sample = db.session.execute(text(sql_sample), {'val': original_val}).fetchone()
    header_text = sample.OriginalValue if sample else "No Header Found"
    
    images = []
    if sample:
        # 2. Get images matching that key (Use col03varchar for filename)
        sql_imgs = """
        SELECT col03varchar 
        FROM GenericDataImport 
        WHERE fn LIKE '%image%' AND keyOriginalValue = :key
        ORDER BY fn
        """
        imgs = db.session.execute(text(sql_imgs), {'key': sample.OriginalValue}).fetchall()
        
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
                         'src': f"/api/tools/inst-corrections/view-image?path={safe_path}",
                         'path': full_disk_path 
                     })

    return jsonify({
        'success': True, 
        'images': images,
        'header_text': header_text
    })

@inst_type_bp.route('/api/tools/inst-corrections/view-image', methods=['GET'])
@login_required
def view_correction_image():
    """Proxy endpoint to view images from arbitrary local paths (Admin Only)."""
    if current_user.role != 'admin': return "Unauthorized", 403
    
    file_path = request.args.get('path')
    if not file_path: return "No path provided", 400
    
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path):
        return "File not found", 404
        
    try:
        if not Image: return "PIL not installed", 500
        
        with Image.open(file_path) as image:
            if image.mode in ('P', 'CMYK', 'RGBA', 'LA', 'I', 'I;16', '1'):
                image = image.convert('RGB')
            
            img_io = io.BytesIO()
            image.save(img_io, 'JPEG', quality=85)
            img_io.seek(0)
            return send_file(img_io, mimetype='image/jpeg')
                
    except Exception as e:
        return f"Error processing image: {str(e)}", 500

@inst_type_bp.route('/api/tools/inst-corrections/search', methods=['POST'])
@login_required
def search_types():
    data = request.json
    term = data.get('term', '')
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify([])

    tables = get_tables(c.county_name)
    target_table = tables['inst_types'] 
    
    try:
        # 1. Inspect Table Columns
        sql_cols = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = :tb
        """
        cols_res = db.session.execute(text(sql_cols), {'tb': target_table}).fetchall()
        columns = [row[0] for row in cols_res]
        
        if not columns:
            name_col = 'InstTypeName'
            desc_col = None
            type_col = None
            active_col = None
        else:
            name_col = next((c for c in columns if 'insttypename' in c.lower()), None)
            if not name_col:
                name_col = next((c for c in columns if 'name' in c.lower()), columns[0])
            
            desc_col = next((c for c in columns if 'desc' in c.lower() and c != name_col), None)

            type_col = next((c for c in columns if 'record_type' in c.lower() or 'recordtype' in c.lower()), None)
            if not type_col:
                 type_col = next((c for c in columns if 'type' in c.lower() and c not in [name_col, desc_col]), None)
            
            active_col = next((c for c in columns if 'active' in c.lower()), None)

        # 2. Build Dynamic SQL
        select_parts = [f"[{name_col}]"]
        if desc_col: select_parts.append(f"[{desc_col}]")
        if type_col: select_parts.append(f"[{type_col}]")
        
        select_clause = ", ".join(select_parts)
        
        where_conditions = [f"[{name_col}] LIKE :term"]
        if desc_col: where_conditions.append(f"[{desc_col}] LIKE :term")
        if type_col: where_conditions.append(f"[{type_col}] LIKE :term")
        
        where_clause = f"({' OR '.join(where_conditions)})"
        
        if active_col:
            where_clause += f" AND [{active_col}] = 1"
        
        sql = f"SELECT TOP 50 {select_clause} FROM [{target_table}] WHERE {where_clause}"
            
        results = db.session.execute(text(sql), {'term': f'%{term}%'}).fetchall()
        
        out = []
        for r in results:
            val_name = r[0]

            val_desc = ''
            idx_tracker = 1
            if desc_col:
                if len(r) > idx_tracker: val_desc = r[idx_tracker]
                idx_tracker += 1
            
            val_type = ''
            if type_col:
                if len(r) > idx_tracker: val_type = r[idx_tracker]
                
            out.append({'name': val_name, 'desc': val_desc, 'type': val_type})
            
        return jsonify(out)

    except Exception as e:
        current_app.logger.error(f"InstType Search Error: {str(e)}")
        return jsonify([])

@inst_type_bp.route('/api/tools/inst-corrections/save', methods=['POST'])
@login_required
def save_correction():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data['county_id'])
    tables = get_tables(c.county_name)
    
    try:
        # 1. Update Corrections Table 
        # Match using Original Value (which never changes)
        sql_log = f"""
        UPDATE {tables['corrections']} 
        SET CorrectedCol03Varchar = :new 
        WHERE OriginalCol03Varchar = :old
        """
        db.session.execute(text(sql_log), {'new': data['corrected'], 'old': data['original']})
        
        # 2. Update Generic Data Import (Headers Only)
        # Use instTypeOriginal to identify rows that were ORIGINALLY this type
        sql_data = """
        UPDATE GenericDataImport 
        SET col03varchar = :new 
        WHERE instTypeOriginal = :old AND fn LIKE '%header%'
        """
        db.session.execute(text(sql_data), {'new': data['corrected'], 'old': data['original']})
        
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})