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

initial_prep_bp = Blueprint('initial_preparation', __name__)

def generate_prep_sql(county_name, book_start=None, book_end=None, use_path=False, image_path_prefix=''):
    """
    Generates the SQL script steps for the Initial Preparation Tool.
    """
    steps = []
    
    def rename_table(sql_content, old_table, new_suffix):
        new_table = f"{county_name}_keli_{new_suffix}"
        return sql_content.replace(old_table, new_table)

    header = f"""
    -- ===============================================
    -- GSI INITIAL PREPARATION SCRIPT
    -- Target County: {county_name}
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

    # 5. POPULATE ORIGINAL INSTRUMENT TYPE
    sql_inst_orig = """
    UPDATE GenericDataImport 
    SET instTypeOriginal = col03varchar 
    WHERE instTypeOriginal IS NULL OR instTypeOriginal = '';
    """
    steps.append(('Preserving Original Instrument Types', sql_inst_orig))

    # 6. INSTRUMENT ID GENERATION
    sql_inst = """
    DECLARE @numbering TABLE (id INT IDENTITY(1,1), fn VARCHAR(200), col01varchar VARCHAR(200));
    INSERT INTO @numbering (fn, col01varchar) 
    SELECT DISTINCT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference',''), CAST(col01varchar AS INT) 
    FROM GenericDataImport 
    ORDER BY REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference',''), CAST(col01varchar AS INT);
    
    UPDATE GenericDataImport SET instrumentid = numb.id 
    FROM GenericDataImport, @numbering numb 
    WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(GenericDataImport.fn,'Header',''),'Legal',''),'Name',''),'Image',''), 'Reference','') = numb.fn 
      AND GenericDataImport.col01varchar = numb.col01varchar;
    """
    steps.append(('Generating Instrument IDs', sql_inst))

    # 7. UPDATE STECH IMAGE PATH (Moved from Keli Linkup)
    if use_path and image_path_prefix:
        sql_path = f"UPDATE GenericDataImport SET stech_image_path = '{image_path_prefix}' + col03varchar WHERE fn LIKE '%image%'"
        steps.append(('Setting Stech Image Paths', sql_path))
        
        # 8. SYNC PATH TO HEADER (New Step)
        # We must sync this to the header so the "Other" legals (created next) inherit it.
        sql_sync = """
        UPDATE a 
        SET stech_image_path = b.stech_image_path 
        FROM GenericDataImport a, GenericDataImport b 
        WHERE b.fn LIKE '%image%' 
          AND a.fn LIKE '%header%'
          AND a.instrumentid = b.instrumentid
          AND b.stech_image_path IS NOT NULL 
          AND b.stech_image_path <> ''
        """
        steps.append(('Syncing Image Paths to Headers', sql_sync))

    # 9. INSERT 'OTHER' LEGALS
    # Now that path is on the Header (from Step 8), this insert will include the correct path.
    sql_legal = """
    INSERT INTO GenericDataImport (fn, col01varchar, stech_image_path, legal_type, col20other, deleteFlag, instrumentid) 
    SELECT REPLACE(fn, 'HEADER', 'Legal'), col01varchar, stech_image_path, 'Other', 'NO LEGAL', 'FALSE', instrumentid 
    FROM GenericDataImport 
    WHERE fn LIKE '%HEADER%' AND deleteFlag = 'FALSE' 
      AND instrumentid NOT IN (SELECT instrumentid FROM GenericDataImport WHERE fn LIKE '%legal%' AND deleteFlag = 'FALSE');
    """
    steps.append(('Inserting Placeholder Legals', sql_legal))

    # 10. KEY ORIGINAL VALUE - HEADER CLEANUP
    sql_kov_header = """
    UPDATE GenericDataImport SET keyOriginalValue = '' WHERE fn LIKE '%header%';
    """
    steps.append(('Initializing Header KeyOriginalValue', sql_kov_header))

    # 11. KEY ORIGINAL VALUE - ASSIGNMENT
    sql_kov_assign = """
    UPDATE a 
    SET keyOriginalValue = b.OriginalValue 
    FROM GenericDataImport a, GenericDataImport b 
    WHERE a.fn NOT LIKE '%header%' 
      AND b.fn LIKE '%header%' 
      AND a.instrumentID = b.instrumentID;
    """
    steps.append(('Populating KeyOriginalValue', sql_kov_assign))

    # --- EXTERNAL TABLES ---
    raw_manifest = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'fromkellprocombined_manifest')
    CREATE TABLE fromkellprocombined_manifest (id varchar(255), book varchar(20), page varchar(20), path varchar(255))
    """
    steps.append(('Creating Manifest Table', rename_table(raw_manifest, 'fromkellprocombined_manifest', 'combined_manifest')))

    raw_merge = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'InstTypesMerge') DROP TABLE InstTypesMerge\nCREATE TABLE InstTypesMerge (Abbr varchar(1000), tFull varchar(1000))"
    steps.append(('Creating InstTypesMerge', raw_merge))

    raw_additions = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliAdditionsExternals') DROP TABLE KeliAdditionsExternals\nCREATE TABLE KeliAdditionsExternals (AddID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, AdditionName VARCHAR(500))"
    steps.append(('Creating Additions Externals', rename_table(raw_additions, 'KeliAdditionsExternals', 'Additions_Externals')))

    raw_inst_types = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliInstTypesExternals') DROP TABLE KeliInstTypesExternals\nCREATE TABLE KeliInstTypesExternals (InstID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, InstTypeName VARCHAR(500))"
    steps.append(('Creating InstTypes Externals', rename_table(raw_inst_types, 'KeliInstTypesExternals', 'InstTypes_Externals')))

    raw_series = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliSeriesExternals') DROP TABLE KeliSeriesExternals\nCREATE TABLE KeliSeriesExternals (SeriesID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, SeriesDate VARCHAR(100))"
    steps.append(('Creating Series Externals', rename_table(raw_series, 'KeliSeriesExternals', 'Series_Externals')))

    raw_tr = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'KeliTownshipRangeExternals') DROP TABLE KeliTownshipRangeExternals\nCREATE TABLE KeliTownshipRangeExternals (TownshipRangeID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, Township VARCHAR(100), Range VARCHAR(100), Active INT)"
    steps.append(('Creating TownshipRange Externals', rename_table(raw_tr, 'KeliTownshipRangeExternals', 'TownshipRange_Externals')))
    
    return steps

@initial_prep_bp.route('/api/tools/initial-prep/preview', methods=['POST'])
@login_required
def preview_prep():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data.get('county_id'))
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    
    steps = generate_prep_sql(c.county_name, data.get('book_start'), data.get('book_end'), data.get('use_path'), data.get('image_path_prefix'))
    full_script = "\n".join([s[1] for s in steps])
    return jsonify({'success': True, 'sql': full_script})

@initial_prep_bp.route('/api/tools/initial-prep/download-sql', methods=['POST'])
@login_required
def download_sql():
    if current_user.role != 'admin': return Response("Unauthorized", 403)
    data = request.json
    c = db.session.get(IndexingCounties, data.get('county_id'))
    if not c: return Response("County not found", 404)

    steps = generate_prep_sql(c.county_name, data.get('book_start'), data.get('book_end'), data.get('use_path'), data.get('image_path_prefix'))
    
    def generate():
        for name, sql in steps:
            yield f"-- STEP: {name}\n{sql}\nGO\n\n"
            
    filename = f"Initial_Prep_{c.county_name}.sql"
    return Response(stream_with_context(generate()), mimetype='application/sql', headers={'Content-Disposition': f'attachment; filename={filename}'})

@initial_prep_bp.route('/api/tools/initial-prep/execute', methods=['POST'])
@login_required
def execute_prep():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    data = request.json
    c = db.session.get(IndexingCounties, data.get('county_id'))
    if not c: return jsonify({'success': False, 'message': 'County not found'})

    steps = generate_prep_sql(c.county_name, data.get('book_start'), data.get('book_end'), data.get('use_path'), data.get('image_path_prefix'))
    total_steps = len(steps)
    
    def generate_stream():
        yield json.dumps({'type': 'start', 'message': f'Starting Preparation for {c.county_name}...'}) + '\n'
        try:
            with db.session.begin():
                for i, (name, sql_command) in enumerate(steps):
                    db.session.execute(text(sql_command))
                    percent = int(((i + 1) / total_steps) * 100)
                    yield json.dumps({'type': 'progress', 'percent': percent, 'message': f"{name}..."}) + '\n'
                    time.sleep(0.2)
            yield json.dumps({'type': 'complete', 'message': 'Preparation Completed Successfully.'}) + '\n'
        except Exception as e:
            yield json.dumps({'type': 'error', 'message': format_error(e)}) + '\n'

    return Response(stream_with_context(generate_stream()), mimetype='application/json')

@initial_prep_bp.route('/api/tools/initial-prep/get-book-range/<int:county_id>', methods=['GET'])
@login_required
def get_book_range(county_id):
    # (Same as before)
    if current_user.role != 'admin': return jsonify({'success': False})
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False})
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        if not s: return jsonify({'success': False})
        path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'Images')
        if not os.path.exists(path): return jsonify({'success': True, 'start': '', 'end': '', 'found': False})
        folders = sorted([f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))])
        if not folders: return jsonify({'success': True, 'start': '', 'end': '', 'found': False})
        return jsonify({'success': True, 'start': folders[0], 'end': folders[-1], 'found': True})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})