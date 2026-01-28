import os
import json
import io
import urllib.parse
from flask import Blueprint, request, jsonify, current_app, send_file
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties

# Try to import PIL for image serving
try:
    from PIL import Image
except ImportError:
    Image = None

missing_names_bp = Blueprint('missing_names_corrections', __name__)

@missing_names_bp.route('/api/tools/missing-names/init', methods=['POST'])
@login_required
def init_tool():
    # [GSI_BLOCK: mn_init_tool]
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    try:
        # Tag missing names
        sql_tag = """
        UPDATE GenericDataImport 
        SET change_script_locations = 'Missing Names Corrections'
        WHERE fn LIKE '%Name%' 
          AND (col03varchar IS NULL OR LEN(LTRIM(RTRIM(col03varchar))) = 0)
          AND deleteFlag = 'FALSE'
        """
        db.session.execute(text(sql_tag))
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_init_tool]

@missing_names_bp.route('/api/tools/missing-names/list', methods=['POST'])
@login_required
def get_list():
    # [GSI_BLOCK: mn_get_list]
    try:
        # 1. Fetch Missing Records
        sql_missing = """
        SELECT id, col02varchar as type, col03varchar as name, instrumentid, keyOriginalValue
        FROM GenericDataImport
        WHERE fn LIKE '%Name%' 
          AND (col03varchar IS NULL OR LEN(LTRIM(RTRIM(col03varchar))) = 0)
          AND deleteFlag = 'FALSE'
        ORDER BY instrumentid
        """
        missing_rows = db.session.execute(text(sql_missing)).fetchall()
        
        if not missing_rows:
            return jsonify({'success': True, 'records': []})

        # 2. Fetch Related Valid Names (Optimized Bulk Fetch)
        sql_related = """
        SELECT instrumentid, col02varchar as type, col03varchar as name
        FROM GenericDataImport
        WHERE fn LIKE '%Name%'
          AND deleteFlag = 'FALSE'
          AND col03varchar IS NOT NULL 
          AND LEN(LTRIM(RTRIM(col03varchar))) > 0
          AND instrumentid IN (
                SELECT DISTINCT instrumentid 
                FROM GenericDataImport 
                WHERE fn LIKE '%Name%' 
                  AND (col03varchar IS NULL OR LEN(LTRIM(RTRIM(col03varchar))) = 0)
                  AND deleteFlag = 'FALSE'
          )
        """
        related_rows = db.session.execute(text(sql_related)).fetchall()

        # 3. Map InstrumentID -> List of Names
        related_map = {}
        for row in related_rows:
            if row.instrumentid:
                if row.instrumentid not in related_map: related_map[row.instrumentid] = []
                # Format: "Grantor: John Doe"
                related_map[row.instrumentid].append(f"{row.type or 'Unknown'}: {row.name}")

        # 4. Merge Data
        records = []
        for r in missing_rows:
            inst_id = r.instrumentid
            # Join multiple names with a pipe
            rel_info = " | ".join(related_map.get(inst_id, [])) if inst_id else ""
            
            records.append({
                'id': r.id, 
                'type': r.type or 'Unknown', 
                'name': r.name,
                'inst_id': inst_id,
                'key_val': r.keyOriginalValue,
                'related_names': rel_info
            })

        return jsonify({'success': True, 'records': records})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_get_list]

@missing_names_bp.route('/api/tools/missing-names/images', methods=['POST'])
@login_required
def get_images():
    # [GSI_BLOCK: mn_get_images]
    try:
        record_id = request.json.get('record_id')
        
        # Fetch path directly from the record
        sql = "SELECT stech_image_path FROM GenericDataImport WHERE id = :id"
        row = db.session.execute(text(sql), {'id': record_id}).fetchone()
        
        if not row or not row[0]:
            return jsonify({'success': False, 'images': [], 'message': 'No image path found.'})
        
        full_path = row[0]
        safe_path = urllib.parse.quote(full_path)
        
        images = [{
            'src': f"/api/tools/missing-names/view-image?path={safe_path}",
            'name': os.path.basename(full_path)
        }]
        
        return jsonify({'success': True, 'images': images})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_get_images]

@missing_names_bp.route('/api/tools/missing-names/view-image', methods=['GET'])
@login_required
def view_image():
    # [GSI_BLOCK: mn_view_image]
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
    # [GSI_END: mn_view_image]

@missing_names_bp.route('/api/tools/missing-names/save', methods=['POST'])
@login_required
def save_correction():
    # [GSI_BLOCK: mn_save]
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    record_id = data.get('id')
    new_name = data.get('value')
    is_reverse = data.get('reverse')
    
    if not record_id: return jsonify({'success': False, 'message': 'No ID'})

    try:
        # 1. Update Name & Script Location
        sql_update = """
        UPDATE GenericDataImport 
        SET col03varchar = :name,
            change_script_locations = 'Missing Names Corrections'
        WHERE id = :id
        """
        db.session.execute(text(sql_update), {'name': new_name, 'id': record_id})
        
        # 2. Handle Reverse Logic
        if is_reverse:
            # Flip current record
            sql_flip_current = """
            UPDATE GenericDataImport 
            SET col02varchar = CASE 
                WHEN col02varchar = 'Grantor' THEN 'Grantee'
                WHEN col02varchar = 'Grantee' THEN 'Grantor'
                ELSE col02varchar END
            WHERE id = :id
            """
            db.session.execute(text(sql_flip_current), {'id': record_id})

            # Flip siblings
            sql_get_inst = "SELECT instrumentid FROM GenericDataImport WHERE id = :id"
            inst_row = db.session.execute(text(sql_get_inst), {'id': record_id}).fetchone()
            
            if inst_row and inst_row.instrumentid:
                sql_flip_others = """
                UPDATE GenericDataImport 
                SET col02varchar = CASE 
                    WHEN col02varchar = 'Grantor' THEN 'Grantee'
                    WHEN col02varchar = 'Grantee' THEN 'Grantor'
                    ELSE col02varchar END
                WHERE instrumentid = :inst_id 
                  AND fn LIKE '%Name%' 
                  AND id <> :current_id
                """
                db.session.execute(text(sql_flip_others), {'inst_id': inst_row.instrumentid, 'current_id': record_id})

        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_save]

@missing_names_bp.route('/api/tools/missing-names/skip', methods=['POST'])
@login_required
def skip_record():
    # [GSI_BLOCK: mn_skip]
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    record_id = data.get('id')
    
    try:
        sql_get_inst = "SELECT instrumentid, col02varchar FROM GenericDataImport WHERE id = :id"
        current_row = db.session.execute(text(sql_get_inst), {'id': record_id}).fetchone()
        
        if not current_row: return jsonify({'success': False, 'message': 'Record not found'})
        inst_id = current_row.instrumentid
        
        # Check siblings
        sql_check_siblings = """
        SELECT TOP 1 * FROM GenericDataImport 
        WHERE instrumentid = :inst_id 
          AND fn LIKE '%Name%' 
          AND col03varchar IS NOT NULL 
          AND LEN(LTRIM(RTRIM(col03varchar))) > 0
          AND deleteFlag = 'FALSE'
        """
        sibling = db.session.execute(text(sql_check_siblings), {'inst_id': inst_id}).fetchone()
        
        if sibling:
            # COPY Sibling
            new_type = 'Grantor' if sibling.col02varchar == 'Grantee' else 'Grantee'
            
            sql_insert = """
            INSERT INTO GenericDataImport (
                fn, col01varchar, col02varchar, col03varchar, 
                instrumentid, keyOriginalValue, stech_image_path, deleteFlag, change_script_locations
            )
            SELECT 
                fn, col01varchar, :new_type, col03varchar, 
                instrumentid, keyOriginalValue, stech_image_path, 'FALSE', 'Missing Names Corrections Auto-Fill'
            FROM GenericDataImport 
            WHERE id = :sib_id
            """
            db.session.execute(text(sql_insert), {'new_type': new_type, 'sib_id': sibling.id})
            
            # Delete current
            db.session.execute(text("UPDATE GenericDataImport SET deleteFlag = 'TRUE' WHERE id = :id"), {'id': record_id})
            
        else:
            # DELETE ALL
            sql_delete_all = """
            UPDATE GenericDataImport 
            SET deleteFlag = 'TRUE' 
            WHERE instrumentid = :inst_id 
              AND (
                  fn LIKE '%Header%' OR 
                  fn LIKE '%Legal%' OR 
                  fn LIKE '%Name%' OR 
                  fn LIKE '%Image%' OR 
                  fn LIKE '%Ref%'
              )
            """
            db.session.execute(text(sql_delete_all), {'inst_id': inst_id})
            
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_skip]