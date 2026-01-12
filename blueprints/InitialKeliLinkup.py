import json
import time
import datetime
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties
from utils import format_error

initial_linkup_bp = Blueprint('initial_keli_linkup', __name__)

def generate_linkup_sql(county_name, book_start=None, book_end=None, image_path_prefix=''):
    """
    Generates the SQL script steps for the Initial Keli Linkup Tool.
    Dynamically renames Keli tables and injects parameters.
    """
    steps = []
    
    # --- HELPER: Dynamic Table Renaming ---
    def process_sql(sql_content, step_name):
        # 1. Rename External Tables to match SetupKeliTables.py convention
        # Pattern: {CountyName}_keli_{Suffix}
        
        # fromkellproinstrument_types -> instrument_types
        sql_content = sql_content.replace('fromkellproinstrument_types', f"{county_name}_keli_instrument_types")
        
        # fromkellproadditions -> additions
        sql_content = sql_content.replace('fromkellproadditions', f"{county_name}_keli_additions")
        
        # fromkellprocombined_manifest -> combined_manifest
        sql_content = sql_content.replace('fromkellprocombined_manifest', f"{county_name}_keli_combined_manifest")
        
        # KeliInstTypesExternals -> InstTypes_Externals (Created in InitialPrep)
        sql_content = sql_content.replace('KeliInstTypesExternals', f"{county_name}_keli_InstTypes_Externals")
        
        # KeliPagesInternal -> pages_internal (Assumed standard CSV name)
        sql_content = sql_content.replace('KeliPagesInternal', f"{county_name}_keli_pages_internal")

        # 2. Inject Parameters
        # Handle Book Range {0}, {1}
        if '{0}' in sql_content and book_start is not None:
            # Check if it also needs {1} (End)
            if '{1}' in sql_content and book_end is not None:
                sql_content = sql_content.replace('{0}', str(book_start)).replace('{1}', str(book_end))
            # Handle Single Parameter (Image Path)
            else:
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

    sql_1 = """
    --Update GenericDataImport instrument_type_internal_id
    UPDATE a SET instrument_type_internal_id = isnull(b.id,'') FROM GenericDataImport a LEFT JOIN fromkellproinstrument_types b ON a.col03varchar = name
    WHERE a.fn LIKE '%header%' AND b.record_type = 'Instrument' AND b.active = '1'
    """
    steps.append(process_sql(sql_1, 'Linking Internal IDs (Active)'))

    sql_2 = """
    --Update GenericDataImport instrument_type_internal_id Inactives
    UPDATE a SET instrument_type_internal_id = isnull(b.id,'') FROM GenericDataImport a LEFT JOIN fromkellproinstrument_types b ON a.col03varchar = name
    WHERE a.fn LIKE '%header%' AND b.record_type = 'Instrument' AND instrument_type_internal_id = ''
    """
    steps.append(process_sql(sql_2, 'Linking Internal IDs (Inactive)'))

    sql_3 = """
    --Update GenericDataImport instrument_type_internal_id Cleanup
    UPDATE GenericDataImport SET instrument_type_internal_id = '' WHERE fn LIKE '%header%' AND instrument_type_internal_id = '0'
    """
    steps.append(process_sql(sql_3, 'Cleaning Internal IDs'))

    sql_4 = """
    --Update GenericDataImport instrument_type_external_id
    UPDATE a SET instrument_type_external_id = isnull(b.InstID,'') FROM GenericDataImport a LEFT JOIN KeliInstTypesExternals b ON a.col03varchar = b.InstTypeName
    WHERE a.fn LIKE '%header%'
    """
    steps.append(process_sql(sql_4, 'Linking External IDs'))

    sql_5 = """
    --Update GenericDataImport instrument_type_external_id Cleanup
    UPDATE GenericDataImport SET instrument_type_external_id = '' WHERE fn LIKE '%header%' AND instrument_type_external_id = '0'
    """
    steps.append(process_sql(sql_5, 'Cleaning External IDs'))

    sql_6 = """
    --Update GenericDataImport addition_internal_id
    UPDATE a SET addition_internal_id = b.id FROM GenericDataImport a LEFT JOIN fromkellproadditions b ON a.col05varchar = b.name WHERE a.fn LIKE '%legal%' AND b.name != ''
    update a set addition_internal_id = b.id from GenericDataImport a left join fromkellproadditions b on a.col05varchar = replace(b.name, ',', '') where a.fn like '%legal%' 
    and legal_type = 'Platted' and addition_internal_id = '' and b.name != ''
    """
    steps.append(process_sql(sql_6, 'Linking Addition IDs'))

    if book_start and book_end:
        sql_7 = """
        --Update GenericDataImport key_id Alternate (Retired)
        update GenericDataImport set key_id = case when len(col03varchar) < 20 then ''
            when len(reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)),'FIT.',''))) = 23
            then reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)),'FIT.',''))
            else reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)-1),'FIT.',''))
        end where fn like '%image%' and book between '{0}' and '{1}'
        """
        steps.append(process_sql(sql_7, 'Updating Image Key IDs (Legacy)'))

    sql_8 = """
    --Update GenericDataImport page_number
    UPDATE GenericDataImport SET page_number = RIGHT('0000' + col02varchar, 5 - isnumeric(col02varchar)) WHERE fn LIKE '%image%'
    """
    steps.append(process_sql(sql_8, 'Formatting Page Numbers'))

    sql_9 = """
    --Update GenericDataImport book
    UPDATE GenericDataImport SET book = SUBSTRING(col03varchar, 0, 7) WHERE fn LIKE '%image%'
    """
    steps.append(process_sql(sql_9, 'Formatting Book Numbers'))

    # --- BATCH 2 QUERIES ---

    if image_path_prefix:
        sql_10 = """
        --Update GenericDataImport stech_image_path
        UPDATE GenericDataImport SET stech_image_path = '{0}' + col03varchar where fn like '%image%'
        """
        steps.append(process_sql(sql_10, 'Setting Stech Image Paths'))

    if book_start and book_end:
        # Note: Using raw string r'' for backslash safety in python
        sql_11 = r"""
        --Update GenericDataImport keli_image_path From Combined Manifest (If Images Come From KellPro)
        update a set keli_image_path = b.id from GenericDataImport a, fromkellprocombined_manifest b where a.fn like '%image%' and keli_image_path = '' and b.book between '{0}' and '{1}' and a.col03varchar = replace(replace(b.path, 'MS', '00'), '/', '\')
        """
        steps.append(process_sql(sql_11, 'Linking Combined Manifest IDs'))

        sql_12 = """
        --Update GenericDataImport key_id (Retired)
        UPDATE a SET key_id = b.id FROM GenericDataImport a, KeliPagesInternal b WHERE fn LIKE '%image%' and a.book between '{0}' and '{1}' AND a.key_id = b.key_id
        """
        steps.append(process_sql(sql_12, 'Linking Internal Pages Key IDs'))

    sql_13 = """
    --Update GenericDataImport stech_image_path
    UPDATE a SET stech_image_path = b.stech_image_path from GenericDataImport a, GenericDataImport b where b.fn like '%image%' and a.instrumentid = b.instrumentid
    """
    steps.append(process_sql(sql_13, 'Syncing Stech Paths to Instruments'))
    
    return steps

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
        data.get('book_start'), 
        data.get('book_end'),
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
        data.get('book_start'), 
        data.get('book_end'),
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