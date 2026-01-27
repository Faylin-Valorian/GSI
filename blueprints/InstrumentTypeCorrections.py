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

# [GSI_BLOCK: inst_get_tables]
def get_tables(county_name):
    return {
        'corrections': f"{county_name}_Instrument_Type_Corrections",
        'inst_types': f"{county_name}_keli_instrument_types"
    }
# [GSI_END: inst_get_tables]

@inst_type_bp.route('/api/tools/inst-corrections/init', methods=['POST'])
@login_required
def init_tool():
    # [GSI_BLOCK: inst_init_tool]
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
    # [GSI_END: inst_init_tool]

@inst_type_bp.route('/api/tools/inst-corrections/list', methods=['POST'])
@login_required
def get_correction_list():
    # [GSI_BLOCK: inst_get_list]
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
    # [GSI_END: inst_get_list]

@inst_type_bp.route('/api/tools/inst-corrections/images', methods=['POST'])
@login_required
def get_images_for_record():
    # [GSI_BLOCK: inst_get_images]
    county_id = request.json.get('county_id')
    original_val = request.json.get('value')
    relative_base_path = request.json.get('base_path') 
    
    c = db.session.get(IndexingCounties, county_id)
    
    # 1. Find a Header row
    sql_sample = "SELECT TOP 1 OriginalValue FROM GenericDataImport WHERE instTypeOriginal = :val AND fn LIKE '%header%'"
    sample = db.session.execute(text(sql_sample), {'val': original_val}).fetchone()
    header_text = sample.OriginalValue if sample else "No Header Found"
    
    images = []
    if sample:
        # 2. Get images
        sql_imgs = "SELECT col03varchar FROM GenericDataImport WHERE fn LIKE '%image%' AND keyOriginalValue = :key ORDER BY fn"
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
                         'path': full_disk_path,
                         'name': filename
                     })

    return jsonify({
        'success': True, 
        'images': images,
        'header_text': header_text
    })
    # [GSI_END: inst_get_images]

@inst_type_bp.route('/api/tools/inst-corrections/view-image', methods=['GET'])
@login_required
def view_correction_image():
    # [GSI_BLOCK: inst_view_image]
    if current_user.role != 'admin': return "Unauthorized", 403
    file_path = request.args.get('path')
    if not file_path: return "No path provided", 400
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path): return "File not found", 404
    try:
        if not Image: return "PIL not installed", 500
        with Image.open(file_path) as image:
            if image.mode in ('P', 'CMYK', 'RGBA', 'LA', 'I', 'I;16', '1'):
                image = image.convert('RGB')
            img_io = io.BytesIO()
            image.save(img_io, 'JPEG', quality=85)
            img_io.seek(0)
            return send_file(img_io, mimetype='image/jpeg')
    except Exception as e: return f"Error processing image: {str(e)}", 500
    # [GSI_END: inst_view_image]

@inst_type_bp.route('/api/tools/inst-corrections/search', methods=['POST'])
@login_required
def search_types():
    # [GSI_BLOCK: inst_search]
    data = request.json
    term = data.get('term', '')
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify([])

    tables = get_tables(c.county_name)
    target_table = tables['inst_types'] 
    
    try:
        # 1. Get Column Names (to handle case sensitivity)
        sql_cols = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tb"
        cols_res = db.session.execute(text(sql_cols), {'tb': target_table}).fetchall()
        columns = [row[0] for row in cols_res]
        
        if not columns: return jsonify([])

        # 2. Identify Target Columns
        name_col = next((c for c in columns if 'insttypename' == c.lower()), None)
        if not name_col: name_col = next((c for c in columns if 'name' == c.lower()), None)
        if not name_col: name_col = columns[0] # Fallback
        
        desc_col = next((c for c in columns if 'description' == c.lower()), None)
        if not desc_col: desc_col = next((c for c in columns if 'desc' == c.lower()), None)

        type_col = next((c for c in columns if 'record_type' == c.lower()), None)
        if not type_col: type_col = next((c for c in columns if 'recordtype' == c.lower()), None)

        active_col = next((c for c in columns if 'active' == c.lower()), None)

        # 3. Build Select & Where
        select_cols = [f"[{name_col}]"]
        if desc_col: select_cols.append(f"[{desc_col}]")
        if type_col: select_cols.append(f"[{type_col}]")

        where_conds = [f"[{name_col}] LIKE :term"]
        if desc_col: where_conds.append(f"[{desc_col}] LIKE :term")
        
        where_sql = f"({' OR '.join(where_conds)})"
        if active_col: where_sql += f" AND [{active_col}] = 1"
        
        sql = f"SELECT TOP 50 {', '.join(select_cols)} FROM [{target_table}] WHERE {where_sql}"
        
        results = db.session.execute(text(sql), {'term': f'%{term}%'}).fetchall()
        
        # 4. Map Results
        out = []
        for r in results:
            val_name = r[0]
            val_desc = ''
            current_idx = 1
            if desc_col:
                val_desc = r[current_idx] if r[current_idx] else ''
                current_idx += 1
            
            val_type = ''
            if type_col:
                val_type = r[current_idx] if r[current_idx] else ''
            
            out.append({'name': val_name, 'desc': val_desc, 'type': val_type})
            
        return jsonify(out)

    except Exception as e:
        return jsonify([])
    # [GSI_END: inst_search]

@inst_type_bp.route('/api/tools/inst-corrections/save', methods=['POST'])
@login_required
def save_correction():
    # [GSI_BLOCK: inst_save]
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data['county_id'])
    tables = get_tables(c.county_name)
    try:
        db.session.execute(text(f"UPDATE {tables['corrections']} SET CorrectedCol03Varchar = :new WHERE OriginalCol03Varchar = :old"), {'new': data['corrected'], 'old': data['original']})
        db.session.execute(text("UPDATE GenericDataImport SET col03varchar = :new WHERE instTypeOriginal = :old AND fn LIKE '%header%'"), {'new': data['corrected'], 'old': data['original']})
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: inst_save]