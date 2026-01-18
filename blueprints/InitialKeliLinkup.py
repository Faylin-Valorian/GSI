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

initial_linkup_bp = Blueprint('initial_keli_linkup', __name__)

def generate_linkup_sql(county_name, use_book_range=False, book_start=None, book_end=None, use_path=False, image_path_prefix=''):
    """
    Generates the SQL script steps for the Initial Keli Linkup Tool.
    Dynamically renames Keli tables and injects parameters based on toggles.
    """
    steps = []
    
    # --- HELPER: Dynamic Table Renaming ---
    def process_sql(sql_content, step_name):
        # 1. Rename External Tables to match SetupKeliTables.py convention
        sql_content = sql_content.replace('fromkellproinstrument_types', f"{county_name}_keli_instrument_types")
        sql_content = sql_content.replace('fromkellproadditions', f"{county_name}_keli_additions")
        sql_content = sql_content.replace('fromkellprocombined_manifest', f"{county_name}_keli_combined_manifest")
        sql_content = sql_content.replace('KeliInstTypesExternals', f"{county_name}_keli_InstTypes_Externals")
        sql_content = sql_content.replace('KeliPagesInternal', f"{county_name}_keli_pages_internal")

        # 2. Inject Parameters
        if use_book_range and '{0}' in sql_content and book_start is not None:
            if '{1}' in sql_content and book_end is not None:
                sql_content = sql_content.replace('{0}', str(book_start)).replace('{1}', str(book_end))
        
        if use_path and '{0}' in sql_content and image_path_prefix is not None:
             sql_content = sql_content.replace('{0}', str(image_path_prefix))

        return (step_name, sql_content)

    # Header
    header = f"""
    -- ===============================================
    -- GSI INITIAL KELI LINKUP SCRIPT
    -- Target County: {county_name}
    -- Generated: {datetime.datetime.now()}
    -- ===============================================
    """
    steps.append(('Header', header))

    # --- BATCH 1 QUERIES ---
    sql_1 = "UPDATE a SET instrument_type_internal_id = isnull(b.id,'') FROM GenericDataImport a LEFT JOIN fromkellproinstrument_types b ON a.col03varchar = name WHERE a.fn LIKE '%header%' AND b.record_type = 'Instrument' AND b.active = '1'"
    steps.append(process_sql(sql_1, 'Linking Internal IDs (Active)'))

    sql_2 = "UPDATE a SET instrument_type_internal_id = isnull(b.id,'') FROM GenericDataImport a LEFT JOIN fromkellproinstrument_types b ON a.col03varchar = name WHERE a.fn LIKE '%header%' AND b.record_type = 'Instrument' AND instrument_type_internal_id = ''"
    steps.append(process_sql(sql_2, 'Linking Internal IDs (Inactive)'))

    sql_3 = "UPDATE GenericDataImport SET instrument_type_internal_id = '' WHERE fn LIKE '%header%' AND instrument_type_internal_id = '0'"
    steps.append(process_sql(sql_3, 'Cleaning Internal IDs'))

    sql_4 = "UPDATE a SET instrument_type_external_id = isnull(b.InstID,'') FROM GenericDataImport a LEFT JOIN KeliInstTypesExternals b ON a.col03varchar = b.InstTypeName WHERE a.fn LIKE '%header%'"
    steps.append(process_sql(sql_4, 'Linking External IDs'))

    sql_5 = "UPDATE GenericDataImport SET instrument_type_external_id = '' WHERE fn LIKE '%header%' AND instrument_type_external_id = '0'"
    steps.append(process_sql(sql_5, 'Cleaning External IDs'))

    sql_6 = "UPDATE a SET addition_internal_id = b.id FROM GenericDataImport a LEFT JOIN fromkellproadditions b ON a.col05varchar = b.name WHERE a.fn LIKE '%legal%' AND b.name != ''"
    steps.append(process_sql(sql_6, 'Linking Addition IDs'))

    if use_book_range and book_start and book_end:
        sql_7 = "update GenericDataImport set key_id = case when len(col03varchar) < 20 then '' when len(reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)),'FIT.',''))) = 23 then reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)),'FIT.','')) else reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)-1),'FIT.','')) end where fn like '%image%' and book between '{0}' and '{1}'"
        steps.append(process_sql(sql_7, 'Updating Image Key IDs (Legacy)'))

    sql_8 = "UPDATE GenericDataImport SET page_number = RIGHT('0000' + col02varchar, 5 - isnumeric(col02varchar)) WHERE fn LIKE '%image%'"
    steps.append(process_sql(sql_8, 'Formatting Page Numbers'))

    sql_9 = "UPDATE GenericDataImport SET book = SUBSTRING(col03varchar, 0, 7) WHERE fn LIKE '%image%'"
    steps.append(process_sql(sql_9, 'Formatting Book Numbers'))

    # --- BATCH 2 QUERIES ---
    if use_path and image_path_prefix:
        sql_10 = "UPDATE GenericDataImport SET stech_image_path = '{0}' + col03varchar where fn like '%image%'"
        steps.append(process_sql(sql_10, 'Setting Stech Image Paths'))

    if use_book_range and book_start and book_end:
        sql_11 = r"update a set keli_image_path = b.id from GenericDataImport a, fromkellprocombined_manifest b where a.fn like '%image%' and keli_image_path = '' and b.book between '{0}' and '{1}' and a.col03varchar = replace(replace(b.path, 'MS', '00'), '/', '\')"
        steps.append(process_sql(sql_11, 'Linking Combined Manifest IDs'))

        sql_12 = "UPDATE a SET key_id = b.id FROM GenericDataImport a, KeliPagesInternal b WHERE fn LIKE '%image%' and a.book between '{0}' and '{1}' AND a.key_id = b.key_id"
        steps.append(process_sql(sql_12, 'Linking Internal Pages Key IDs'))

    sql_13 = "UPDATE a SET stech_image_path = b.stech_image_path from GenericDataImport a, GenericDataImport b where b.fn like '%image%' and a.instrumentid = b.instrumentid"
    steps.append(process_sql(sql_13, 'Syncing Stech Paths to Instruments'))
    
    return steps

@initial_linkup_bp.route('/api/tools/initial-keli-linkup/defaults/<int:county_id>', methods=['GET'])
@login_required
def get_linkup_defaults(county_id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False, 'message': 'County not found'})
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        if not s: return jsonify({'success': False, 'message': 'State not found'})
        
        # Construct path
        state_dir = secure_filename(s.state_name)
        county_dir = secure_filename(c.county_name)
        images_path = os.path.join(current_app.root_path, 'data', state_dir, county_dir, 'Images')
        
        book_start = ""
        book_end = ""
        full_path_str = ""
        found = False
        folders_found = 0

        if os.path.exists(images_path):
            found = True
            folders = sorted([f for f in os.listdir(images_path) if os.path.isdir(os.path.join(images_path, f))])
            folders_found = len(folders)
            if folders:
                book_start = folders[0]
                book_end = folders[-1]
            
            full_path_str = images_path
            if not full_path_str.endswith(os.sep):
                full_path_str += os.sep

        # Return Debug Info along with data
        return jsonify({
            'success': True,
            'found': found,
            'path_prefix': full_path_str,
            'book_start': book_start,
            'book_end': book_end,
            'debug_info': {
                'tried_path': images_path,
                'path_exists': found,
                'folders_count': folders_found,
                'state_dir': state_dir,
                'county_dir': county_dir
            }
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@initial_linkup_bp.route('/api/tools/initial-keli-linkup/preview', methods=['POST'])
@login_required
def preview_linkup():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    
    steps = generate_linkup_sql(
        c.county_name, 
        data.get('use_book_range', False),
        data.get('book_start'), 
        data.get('book_end'),
        data.get('use_path', False),
        data.get('image_path_prefix')
    )
    full_script = "\n".join([s[1] for s in steps])
    return jsonify({'success': True, 'sql': full_script})

@initial_linkup_bp.route('/api/tools/initial-keli-linkup/execute', methods=['POST'])
@login_required
def execute_linkup():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})

    steps = generate_linkup_sql(
        c.county_name, 
        data.get('use_book_range', False),
        data.get('book_start'), 
        data.get('book_end'),
        data.get('use_path', False),
        data.get('image_path_prefix')
    )
    total_steps = len(steps)
    
    def generate_stream():
        yield json.dumps({'type': 'start', 'message': f'Starting Keli Linkup for {c.county_name}...'}) + '\n'
        try:
            with db.session.begin():
                for i, (name, sql_command) in enumerate(steps):
                    db.session.execute(text(sql_command))
                    percent = int(((i + 1) / total_steps) * 100)
                    yield json.dumps({'type': 'progress', 'percent': percent, 'message': f"{name}..."}) + '\n'
                    time.sleep(0.1)
            yield json.dumps({'type': 'complete', 'message': 'Linkup Completed Successfully.'}) + '\n'
        except Exception as e:
            yield json.dumps({'type': 'error', 'message': format_error(e)}) + '\n'

    return Response(stream_with_context(generate_stream()), mimetype='application/json')