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
    # Fetch records
    sql = """
    SELECT id, col02varchar as type, col03varchar as name, instrumentid, keyOriginalValue
    FROM GenericDataImport
    WHERE fn LIKE '%Name%' 
      AND (col03varchar IS NULL OR LEN(LTRIM(RTRIM(col03varchar))) = 0)
      AND deleteFlag = 'FALSE'
    ORDER BY instrumentid
    """
    try:
        results = db.session.execute(text(sql)).fetchall()
        return jsonify({
            'success': True,
            'records': [{
                'id': r.id, 
                'type': r.type or 'Unknown', 
                'name': r.name, 
                'inst_id': r.instrumentid,
                'key_val': r.keyOriginalValue
            } for r in results]
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    # [GSI_END: mn_get_list]

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