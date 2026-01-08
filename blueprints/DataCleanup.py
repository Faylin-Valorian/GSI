import os
import json
import time
import datetime
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties, IndexingStates
from utils import format_error
from werkzeug.utils import secure_filename

cleanup_bp = Blueprint('data_cleanup', __name__)

def generate_cleanup_sql(book_start=None, book_end=None):
    """Generates the SQL script steps."""
    steps = []
    
    header = f"""
    -- ===============================================
    -- GSI INITIAL DATA PREPARATION SCRIPT
    -- Generated: {datetime.datetime.now()}
    -- ===============================================
    """
    steps.append(('Header', header))

    # 1. BACKUP
    sql_backup = """
    IF OBJECT_ID('dbo.GenericDataImportBackup', 'U') IS NOT NULL DROP TABLE dbo.GenericDataImportBackup;
    SELECT * INTO dbo.GenericDataImportBackup FROM dbo.GenericDataImport;
    """
    steps.append(('Creating Backup', sql_backup))
    
    # 2. FN UPDATES
    sql_fn = """
    UPDATE GenericDataImport SET fn = REPLACE(fn, 'Images', 'Image') WHERE fn LIKE '%Images%';
    UPDATE GenericDataImport SET fn = REPLACE(fn, 'Legals', 'Legal') WHERE fn LIKE '%Legals%';
    UPDATE GenericDataImport SET fn = REPLACE(fn, 'Names', 'Name') WHERE fn LIKE '%Names%';
    """
    steps.append(('Normalizing Filenames', sql_fn))

    # 3. DEFAULTS
    sql_defaults = """
    UPDATE GenericDataImport SET deleteFlag = 'FALSE';
    UPDATE GenericDataImport SET change_script_locations = '';
    """
    steps.append(('Setting Default Flags', sql_defaults))

    # 4. ORIGINAL VALUE CLEANUP
    sql_orig = """
    UPDATE GenericDataImport SET OriginalValue = REPLACE(REPLACE(OriginalValue, '"', ''), ',', '|');
    """
    steps.append(('Cleaning Original Values', sql_orig))

    # 5. INSTRUMENT ID GENERATION
    sql_inst = """
    DECLARE @numbering TABLE (id INT IDENTITY(1,1), fn VARCHAR(200), col01varchar VARCHAR(200));
    
    INSERT INTO @numbering (fn, col01varchar) 
    SELECT DISTINCT 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference',''), 
        CAST(col01varchar AS INT) 
    FROM GenericDataImport 
    ORDER BY 
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference',''), 
        CAST(col01varchar AS INT);
    
    UPDATE GenericDataImport 
    SET instrumentid = numb.id 
    FROM GenericDataImport, @numbering numb 
    WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(GenericDataImport.fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference','') = numb.fn 
      AND GenericDataImport.col01varchar = numb.col01varchar;
    """
    steps.append(('Generating Instrument IDs', sql_inst))

    # 6. INSERT 'OTHER' LEGALS
    sql_legal = """
    INSERT INTO GenericDataImport (fn, col01varchar, stech_image_path, legal_type, col20other, deleteFlag, instrumentid) 
    SELECT 
        REPLACE(fn, 'HEADER', 'Legal'), 
        col01varchar, 
        stech_image_path, 
        'Other', 
        'NO LEGAL', 
        'FALSE', 
        instrumentid 
    FROM GenericDataImport 
    WHERE fn LIKE '%HEADER%' 
      AND deleteFlag = 'FALSE' 
      AND instrumentid NOT IN (SELECT instrumentid FROM GenericDataImport WHERE fn LIKE '%legal%' AND deleteFlag = 'FALSE');
    """
    steps.append(('Inserting Placeholder Legals', sql_legal))

    # 7. IMAGE PATH UPDATE (Optional)
    if book_start and book_end:
        sql_img = f"""
        UPDATE GenericDataImport 
        SET col03varchar = REPLACE(col03varchar, SUBSTRING(col03varchar, CHARINDEX('_', col03varchar), CHARINDEX('.', col03varchar) - CHARINDEX('_', col03varchar)), '') 
        WHERE fn LIKE '%image%' 
          AND book BETWEEN '{book_start}' AND '{book_end}';
        """
        steps.append(('Cleaning Image Paths', sql_img))
    
    return steps

@cleanup_bp.route('/api/tools/cleanup/preview', methods=['POST'])
@login_required
def preview_cleanup():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    data = request.json
    steps = generate_cleanup_sql(data.get('book_start'), data.get('book_end'))
    full_script = "\n".join([s[1] for s in steps])
    return jsonify({'success': True, 'sql': full_script})

@cleanup_bp.route('/api/tools/cleanup/execute', methods=['POST'])
@login_required
def execute_cleanup():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    
    data = request.json
    steps = generate_cleanup_sql(data.get('book_start'), data.get('book_end'))
    total_steps = len(steps)
    
    def generate_stream():
        yield json.dumps({'type': 'start', 'message': 'Starting Database Cleanup...'}) + '\n'
        
        try:
            with db.session.begin():
                for i, (name, sql) in enumerate(steps):
                    db.session.execute(text(sql))
                    percent = int(((i + 1) / total_steps) * 100)
                    yield json.dumps({
                        'type': 'progress',
                        'percent': percent,
                        'message': f"{name}..."
                    }) + '\n'
                    time.sleep(0.2) 

            yield json.dumps({'type': 'complete', 'message': 'Cleanup Completed Successfully.'}) + '\n'
            
        except Exception as e:
            err_msg = format_error(e)
            yield json.dumps({'type': 'error', 'message': err_msg}) + '\n'

    return Response(stream_with_context(generate_stream()), mimetype='application/json')

@cleanup_bp.route('/api/tools/cleanup/get-book-range/<int:county_id>', methods=['GET'])
@login_required
def get_book_range(county_id):
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False})
        
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        if not s: return jsonify({'success': False})
        
        path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'Images')
        
        if not os.path.exists(path): 
            return jsonify({'success': True, 'start': '', 'end': '', 'found': False})
        
        folders = sorted([f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))])
        
        if not folders: 
            return jsonify({'success': True, 'start': '', 'end': '', 'found': False})
        
        return jsonify({'success': True, 'start': folders[0], 'end': folders[-1], 'found': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})