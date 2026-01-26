import os
import json
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties

missing_names_bp = Blueprint('missing_names_corrections', __name__)

@missing_names_bp.route('/api/tools/missing-names/init', methods=['POST'])
@login_required
def init_tool():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    try:
        # Tag missing names (Robust check for whitespace)
        sql_tag = """
        UPDATE GenericDataImport 
        SET change_script_location = 'Missing Names Corrections'
        WHERE fn LIKE '%name%' 
          AND (col03varchar IS NULL OR LTRIM(RTRIM(col03varchar)) = '')
          AND deleteFlag = 'FALSE'
        """
        db.session.execute(text(sql_tag))
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})

@missing_names_bp.route('/api/tools/missing-names/list', methods=['POST'])
@login_required
def get_list():
    # Fetch records marked as missing names
    # Added LTRIM/RTRIM to catch fields that contain only spaces
    sql = """
    SELECT id, col02varchar as type, col03varchar as name, instrumentid
    FROM GenericDataImport
    WHERE fn LIKE '%name%' 
      AND (col03varchar IS NULL OR LTRIM(RTRIM(col03varchar)) = '')
      AND deleteFlag = 'FALSE'
    ORDER BY instrumentid
    """
    try:
        results = db.session.execute(text(sql)).fetchall()
        return jsonify({
            'success': True,
            'records': [{
                'id': r.id, 
                'type': r.type if r.type else 'Unknown', 
                'name': r.name, 
                'inst_id': r.instrumentid
            } for r in results]
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@missing_names_bp.route('/api/tools/missing-names/save', methods=['POST'])
@login_required
def save_correction():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    record_id = data.get('id')
    new_name = data.get('value')
    is_reverse = data.get('reverse')
    
    if not record_id: return jsonify({'success': False, 'message': 'No ID'})

    try:
        # 1. Update the current record's name
        # We also ensure change_script_location is preserved/set as requested
        sql_update = """
        UPDATE GenericDataImport 
        SET col03varchar = :name,
            change_script_location = 'Missing Names Corrections'
        WHERE id = :id
        """
        db.session.execute(text(sql_update), {'name': new_name, 'id': record_id})
        
        # 2. Handle Reverse Logic
        if is_reverse:
            # Flip current record's type
            sql_flip_current = """
            UPDATE GenericDataImport 
            SET col02varchar = CASE 
                WHEN col02varchar = 'Grantor' THEN 'Grantee'
                WHEN col02varchar = 'Grantee' THEN 'Grantor'
                ELSE col02varchar END
            WHERE id = :id
            """
            db.session.execute(text(sql_flip_current), {'id': record_id})

            # Find instrument ID to flip siblings
            sql_get_inst = "SELECT instrumentid FROM GenericDataImport WHERE id = :id"
            inst_row = db.session.execute(text(sql_get_inst), {'id': record_id}).fetchone()
            
            if inst_row and inst_row.instrumentid:
                # Flip all OTHER name records for this instrument
                sql_flip_others = """
                UPDATE GenericDataImport 
                SET col02varchar = CASE 
                    WHEN col02varchar = 'Grantor' THEN 'Grantee'
                    WHEN col02varchar = 'Grantee' THEN 'Grantor'
                    ELSE col02varchar END
                WHERE instrumentid = :inst_id 
                  AND fn LIKE '%name%' 
                  AND id <> :current_id
                """
                db.session.execute(text(sql_flip_others), {'inst_id': inst_row.instrumentid, 'current_id': record_id})

        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})

@missing_names_bp.route('/api/tools/missing-names/skip', methods=['POST'])
@login_required
def skip_record():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    record_id = data.get('id')
    
    try:
        # Get Instrument ID
        sql_get_inst = "SELECT instrumentid, col02varchar FROM GenericDataImport WHERE id = :id"
        current_row = db.session.execute(text(sql_get_inst), {'id': record_id}).fetchone()
        
        if not current_row: return jsonify({'success': False, 'message': 'Record not found'})
        
        inst_id = current_row.instrumentid
        
        # Check for ANY valid name sibling
        sql_check_siblings = """
        SELECT TOP 1 * FROM GenericDataImport 
        WHERE instrumentid = :inst_id 
          AND fn LIKE '%name%' 
          AND col03varchar IS NOT NULL 
          AND LTRIM(RTRIM(col03varchar)) <> '' 
          AND deleteFlag = 'FALSE'
        """
        sibling = db.session.execute(text(sql_check_siblings), {'inst_id': inst_id}).fetchone()
        
        if sibling:
            # SCENARIO A: Sibling exists. Copy it, flip type, insert.
            new_type = 'Grantor' if sibling.col02varchar == 'Grantee' else 'Grantee'
            
            # Insert Copy
            sql_insert = """
            INSERT INTO GenericDataImport (
                fn, col01varchar, col02varchar, col03varchar, 
                instrumentid, keyOriginalValue, stech_image_path, deleteFlag, change_script_location
            )
            SELECT 
                fn, col01varchar, :new_type, col03varchar, 
                instrumentid, keyOriginalValue, stech_image_path, 'FALSE', 'Missing Names Corrections Auto-Fill'
            FROM GenericDataImport 
            WHERE id = :sib_id
            """
            db.session.execute(text(sql_insert), {'new_type': new_type, 'sib_id': sibling.id})
            
            # Delete the current empty placeholder
            db.session.execute(text("UPDATE GenericDataImport SET deleteFlag = 'TRUE' WHERE id = :id"), {'id': record_id})
            
        else:
            # SCENARIO B: No siblings (Both sides missing). Delete EVERYTHING for this instrument.
            sql_delete_all = """
            UPDATE GenericDataImport 
            SET deleteFlag = 'TRUE' 
            WHERE instrumentid = :inst_id 
              AND (
                  fn LIKE '%header%' OR 
                  fn LIKE '%legal%' OR 
                  fn LIKE '%name%' OR 
                  fn LIKE '%image%' OR 
                  fn LIKE '%ref%'
              )
            """
            db.session.execute(text(sql_delete_all), {'inst_id': inst_id})
            
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})