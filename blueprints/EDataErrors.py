import os
import csv
import json
import urllib.parse
import io
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify, send_file
from flask_login import login_required, current_user
from sqlalchemy import text, inspect
from extensions import db
from models import IndexingCounties
from utils import format_error

# Try to import PIL for image serving
try:
    from PIL import Image
except ImportError:
    Image = None

edata_errors_bp = Blueprint('edata_errors', __name__)

# [GSI_BLOCK: edata_errors_constants]
CSV_COLS = """'"ID","FN","OriginalValue","col01varchar","col02varchar","col03varchar","col04varchar","col05varchar","col06varchar","col07varchar","col08varchar","col09varchar","col10varchar","key_id","book","page_number","stech_image_path","keli_image_path","beginning_page","ending_page","record_series_internal_id","record_series_external_id","instrument_type_internal_id","instrument_type_external_id","grantor_suffix_internal_id","grantee_suffix_internal_id","manual_page_count","legal_type","addition_internal_id","addition_external_id","township_range_internal_id","township_range_external_id","col20other","uf1","uf2","uf3","leftovers","instTypeOriginal","keyOriginalValue","deleteFlag","change_script_locations","instrumentid","checked"'"""

DATA_COLS = """'"' + cast(ID as varchar) + '","' + FN + '","' + OriginalValue + '","' + col01varchar + '","' + col02varchar + '","' + col03varchar + '","' + col04varchar + '","' + col05varchar + '","' + col06varchar + '","' + col07varchar + '","' + col08varchar + '","' + col09varchar + '","' + col10varchar + '","' + key_id + '","' + book + '","' + page_number + '","' + stech_image_path + '","' + keli_image_path + '","' + beginning_page + '","' + ending_page + '","' + record_series_internal_id + '","' + record_series_external_id + '","' + instrument_type_internal_id + '","' + instrument_type_external_id + '","' + grantor_suffix_internal_id + '","' + grantee_suffix_internal_id + '","' + manual_page_count + '","' + legal_type + '","' + addition_internal_id + '","' + addition_external_id + '","' + township_range_internal_id + '","' + township_range_external_id + '","' + col20other + '","' + uf1 + '","' + uf2 + '","' + uf3 + '","' + leftovers + '","' + isnull(instTypeOriginal,'') + '","' + keyOriginalValue + '","' + deleteFlag + '","' + change_script_locations + '","' + cast(instrumentid as varchar) + '","0"'"""

BASE_SQL = f"""select {CSV_COLS} as strng, 0 as ord, 0 as instrumentid 
union select {DATA_COLS} as strng, 1 as ord, instrumentid 
from GenericDataImport """
# [GSI_END: edata_errors_constants]

# [GSI_BLOCK: edata_errors_queries]
QUERIES = {
    # --- BATCH 1 ---
    "headerNonNumericPageNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and instrumentid in 
			(select instrumentid from GenericDataImport where fn like '%header%' and isnumeric(left(col05varchar,len(rtrim(col05varchar))))=0)
		and col05varchar not in
			(select col05varchar from GenericDataImport where fn like '%header%' and deleteFlag = 'FALSE' and right(col05varchar,1) like '%[A-Z]')
        order by ord, instrumentid
    """,

    "headerDuplicateBookPageNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and col04varchar + col05varchar in 
            (select col04varchar + col05varchar from GenericDataImport where fn like '%header%' group by col04varchar + col05varchar having count(col04varchar + col05varchar) > 1)
        order by ord, instrumentid
    """,

    "headerDuplicateInstrumentNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and col02varchar + col04varchar in 
            (select distinct col02varchar + col04varchar from GenericDataImport where fn like '%header%' and col02varchar != '' and deleteFlag = 'FALSE' group by col02varchar, col04varchar HAVING count(col02varchar) > 1)
        order by ord, instrumentid
    """,

    "headerIncorrectRecordSeries.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and left(col06varchar,4) in 
            (select distinct left(col06varchar,4) from GenericDataImport where fn like '%header%' group by left(col06varchar,4) having count(left(col06varchar,4)) <= 100)
        order by ord, instrumentid
    """,

    "headerInstrumentNumberSixDigits.csv": """
        where fn LIKE '%header%' and deleteFlag = 'FALSE' AND RIGHT('0000000' + col02varchar,7-isnumeric(col02varchar)) LIKE '%[A-Z]' and col02varchar not in
            (select col02varchar from GenericDataImport where fn like '%header%' and deleteFlag = 'FALSE' and right(col02varchar,1) like '%[A-Z]')
        order by ord, instrumentid
    """,

    "headerMissingBeginningPageNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and instrumentid in 
            (select instrumentid from GenericDataImport where fn like '%header%' and col05varchar < '000000')
        order by ord, instrumentid
    """,

    "headerMissingBookNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and instrumentid in 
            (select instrumentid from GenericDataImport where fn like '%header%' and col04varchar < '000000')
        order by ord, instrumentid
    """,

    "headerMissingInstrumentNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and col02varchar = ''
        order by ord, instrumentid
    """,

    "headerMissingRecordSeries.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and record_series_external_id = '' and left(col06varchar,4) not in
            (select name from fromkellprorecord_series)
        order by ord, instrumentid
    """,

    "headerNonNumericInstrumentNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and instrumentid in 
			(select instrumentid from GenericDataImport where fn like '%header%' and col02varchar != '' and isnumeric(left(col02varchar,len(rtrim(col02varchar)))) = 0 and isnumeric(col02varchar) = 0)
		and col02varchar not in
			(select col02varchar from GenericDataImport where fn like '%header%' and deleteFlag = 'FALSE' and right(col02varchar,1) like '%[A-Z]')
        order by ord, instrumentid
    """,

    # --- BATCH 2 ---
    "legalOutOfRangeSection.csv": """
        where fn like '%legal%' and deleteFlag = 'FALSE' and col02varchar != '' and col02varchar != '?' and cast(col02varchar as int) not between 1 and 36
			or fn like '%legal%' and deleteFlag = 'FALSE' and col02varchar != '' and col02varchar = '?'
        order by ord, instrumentid
    """,

    "headerValidBookRange.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and right('000000' + col04varchar,6) not between '{0}' and '{1}'
        order by ord, instrumentid
    """,

    "imageDuplicateBookPageNumber.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and book + page_number in 
			(select book + page_number from GenericDataImport where fn like '%image%' group by book + page_number having count(col04varchar + col05varchar) > 1)
        order by ord, instrumentid
    """,

    "imageIncorrectBookLength.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and len(SUBSTRING(col03varchar, 0, 7)) != 6
        order by ord, instrumentid
    """,

    "imageIncorrectPageLength.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and len(RIGHT('0000' + col02varchar, 5 - isnumeric(col02varchar))) not between 4 and 5
        order by ord, instrumentid
    """,

    "imageIncorrectPathLength.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and len(reverse(replace(substring(reverse(col03varchar),0,charindex('_',col03varchar)),'FIT.',''))) != 0 and col03varchar not in
			(select col03varchar from GenericDataImport where fn like '%image%' and right(replace(col03varchar, '.TIF', ''),2) like '%[_][0-9]')
        order by ord, instrumentid
    """,

    "imageNonNumericPageNumber.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and isnumeric(left(col02varchar,len(rtrim(col02varchar))))=0 and isnumeric(col02varchar)=0 and col02varchar not in
			(select col02varchar from GenericDataImport where fn like '%image%' and deleteFlag = 'FALSE' and right(col02varchar,1) like '%[A-Z]')
        order by ord, instrumentid
    """,

    "legalOutOfCountyTownshipRanges.csv": """
        where fn like '%legal%' and deleteFlag = 'FALSE' and col03varchar + col04varchar != '' and col03varchar + ',' + col04varchar not in ({0})
        order by ord, instrumentid
    """,

    "legalOutOfRangeQuarterSections.csv": """
        where fn like '%legal%' and deleteFlag = 'FALSE' and col08varchar != '' and col08varchar not in ('N2', 'S2', 'E2', 'W2', 'NE', 'NW', 'SE', 'SW')
        order by ord, instrumentid
    """,

    # --- BATCH 3 ---
    "nameDuplicateNames.csv": """
        where fn like '%name%' and deleteFlag = 'FALSE' and col03varchar + cast(instrumentid as varchar) in
			(select distinct col03varchar + cast(instrumentid as varchar) from GenericDataImport where fn like '%name%' group by col02varchar, col03varchar, instrumentid having count(col03varchar + cast(instrumentid as varchar)) > 1)
        order by ord, instrumentid
    """,

    "nameMissingGrantorGranteeNames.csv": """
        where fn like '%name%' and deleteFlag = 'FALSE' and col03varchar = ''
        order by ord, instrumentid
    """,

    "refRecordedNotWithinBookRange.csv": """
        where fn LIKE '%ref%' and deleteFlag = 'FALSE' AND col20other != '' AND col02varchar NOT IN 
			(SELECT DISTINCT RIGHT('000000' + col04varchar,6) FROM GenericDataImport WHERE fn LIKE '%header%')
        order by ord, instrumentid
    """
}

# --- SPLIT IMAGE OVERRIDES ---
SPLIT_OVERRIDES = {
    "headerDuplicateBookPageNumber.csv": """
        where fn like '%image%' and deleteFlag = 'FALSE' and keyOriginalValue in 
            (select OriginalValue from GenericDataImport where fn like '%header%' group by OriginalValue, col04varchar + col05varchar having count(col04varchar + col05varchar) > 1)
        order by ord, instrumentid
    """,
    "headerDuplicateBookPageNumber.csv": """
        where fn like '%header%' and deleteFlag = 'FALSE' and fn = ''
    """
}
# [GSI_END: edata_errors_queries]

# [GSI_BLOCK: edata_errors_utils]
def parse_townships(townships_str):
    formatted = "''"
    if townships_str:
        try:
            reader = csv.reader([townships_str], quotechar="'", delimiter=',', skipinitialspace=True)
            parts = []
            for row in reader:
                for item in row:
                    if item.strip(): parts.append(f"'{item.strip()}'")
            if parts: formatted = ", ".join(parts)
        except:
            parts = [f"'{t.strip()}'" for t in townships_str.split(',') if t.strip()]
            if parts: formatted = ", ".join(parts)
    return formatted

# [GSI_BLOCK: edata_errors_api]
def get_safe_table_name(county_id, error_key):
    # Security: Ensure error_key is valid
    valid_key = False
    for k in QUERIES.keys():
        if k.replace('.csv', '') == error_key:
            valid_key = True
            break
    if not valid_key: return None
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return None
    
    return f"{c.county_name}_eData_Errors_{error_key}"

@edata_errors_bp.route('/api/tools/edata-errors/records', methods=['POST'])
@login_required
def get_error_records():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    try:
        data = request.json
        table_name = get_safe_table_name(data.get('county_id'), data.get('error_key'))
        if not table_name: return jsonify({'success': False, 'message': 'Invalid context'})
        
        # Check if table exists
        insp = inspect(db.engine)
        if table_name not in insp.get_table_names():
            return jsonify({'success': True, 'records': []})

        sql = f"SELECT id, OriginalValue FROM [{table_name}] ORDER BY id"
        res = db.session.execute(text(sql)).fetchall()
        
        records = [{'id': r.id, 'desc': r.OriginalValue} for r in res]
        return jsonify({'success': True, 'records': records})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@edata_errors_bp.route('/api/tools/edata-errors/record-details', methods=['POST'])
@login_required
def get_error_record_details():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    try:
        data = request.json
        table_name = get_safe_table_name(data.get('county_id'), data.get('error_key'))
        if not table_name: return jsonify({'success': False})
        
        sql = f"""SELECT col01varchar, col02varchar, col03varchar, col04varchar, 
                  col05varchar, col06varchar, col07varchar, col08varchar 
                  FROM [{table_name}] WHERE id = :id"""
        res = db.session.execute(text(sql), {'id': data.get('record_id')}).mappings().fetchone()
        
        if res:
            return jsonify({'success': True, 'record': dict(res)})
        return jsonify({'success': False, 'message': 'Record not found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@edata_errors_bp.route('/api/tools/edata-errors/save-record', methods=['POST'])
@login_required
def save_error_record():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    try:
        data = request.json
        table_name = get_safe_table_name(data.get('county_id'), data.get('error_key'))
        if not table_name: return jsonify({'success': False})
        
        fields = data.get('fields', {})
        
        # Build Update SQL dynamically for cols 1-8
        set_clauses = []
        params = {'id': data.get('record_id')}
        
        for i in range(1, 9):
            col = f"col0{i}varchar"
            if col in fields:
                set_clauses.append(f"{col} = :p{i}")
                params[f"p{i}"] = fields[col]
        
        if not set_clauses: return jsonify({'success': True}) # Nothing to update
        
        sql = f"UPDATE [{table_name}] SET {', '.join(set_clauses)} WHERE id = :id"
        db.session.execute(text(sql), params)
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)})

@edata_errors_bp.route('/api/tools/edata-errors/get-image', methods=['POST'])
@login_required
def get_error_image():
    if current_user.role != 'admin': return jsonify({'success': False}), 403
    try:
        data = request.json
        table_name = get_safe_table_name(data.get('county_id'), data.get('error_key'))
        if not table_name: return jsonify({'success': False})
        
        sql = f"SELECT stech_image_path FROM [{table_name}] WHERE id = :id"
        res = db.session.execute(text(sql), {'id': data.get('record_id')}).fetchone()
        
        if not res or not res[0]:
            return jsonify({'success': False, 'images': [], 'message': 'No image path found'})
            
        full_disk_path = res[0]
        safe_path = urllib.parse.quote(full_disk_path)
        
        images = [{
            'src': f"/api/tools/legal-others/view-image?path={safe_path}", # Reusing existing viewer proxy
            'name': os.path.basename(full_disk_path)
        }]
        return jsonify({'success': True, 'images': images})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@edata_errors_bp.route('/api/tools/edata-errors/status/<int:county_id>', methods=['GET'])
@login_required
def get_edata_errors_status(county_id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False, 'message': 'County not found'})

        active_tables = []
        insp = inspect(db.engine)
        all_tables = insp.get_table_names()
        
        for key in QUERIES.keys():
            clean_name = key.replace('.csv', '')
            table_name = f"{c.county_name}_eData_Errors_{clean_name}"
            
            if table_name in all_tables:
                count_sql = f"SELECT count(*) FROM [{table_name}]"
                try:
                    row_count = db.session.execute(text(count_sql)).scalar()
                    if row_count > 0:
                        active_tables.append(clean_name)
                except:
                    pass 

        return jsonify({'success': True, 'active_errors': active_tables})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@edata_errors_bp.route('/api/tools/edata-errors/scan', methods=['POST'])
@login_required
def scan_edata_errors():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    book_start = data.get('book_start', '000000')
    book_end = data.get('book_end', '999999')
    townships = data.get('townships', '')
    
    # [CHANGED] Load Split Setting from DB
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})
    
    is_split_mode = c.is_split_job # Use DB config

    formatted_townships = parse_townships(townships)
    
    def generate_scan_stream():
        yield json.dumps({'type': 'start', 'message': f'Starting Error Scan for {c.county_name} (Split Mode: {is_split_mode})...'}) + '\n'
        
        try:
            total = len(QUERIES)
            count = 0
            tables_created = 0

            # Pre-calc replacements
            record_series_tbl = f"{c.county_name}_keli_record_series"
            inst_types_tbl = f"{c.county_name}_keli_instrument_types"
            additions_tbl = f"{c.county_name}_keli_additions"
            manifest_tbl = f"{c.county_name}_keli_combined_manifest"

            with db.session.begin():
                for filename, where_clause in QUERIES.items():
                    count += 1
                    clean_name = filename.replace('.csv', '')
                    target_table = f"{c.county_name}_eData_Errors_{clean_name}"
                    
                    db.session.execute(text(f"IF OBJECT_ID('[{target_table}]', 'U') IS NOT NULL DROP TABLE [{target_table}]"))
                    
                    # [CHANGED] Apply Split Logic
                    if is_split_mode and filename in SPLIT_OVERRIDES:
                         where_clause = SPLIT_OVERRIDES[filename]
                    
                    # Removing "order by" for SELECT INTO
                    clean_where = where_clause.split('order by')[0]
                    
                    final_sql = f"SELECT * INTO [{target_table}] FROM GenericDataImport {clean_where}"

                    if '{0}' in final_sql and '{1}' in final_sql:
                        final_sql = final_sql.replace('{0}', str(book_start)).replace('{1}', str(book_end))
                    elif '{0}' in final_sql:
                        final_sql = final_sql.replace('{0}', formatted_townships)
                    
                    final_sql = final_sql.replace('fromkellprorecord_series', record_series_tbl)
                    final_sql = final_sql.replace('fromkellproinstrument_types', inst_types_tbl)
                    final_sql = final_sql.replace('fromkellproadditions', additions_tbl)
                    final_sql = final_sql.replace('fromkellprocombined_manifest', manifest_tbl)

                    try:
                        db.session.execute(text(final_sql))
                        row_count = db.session.execute(text(f"SELECT COUNT(*) FROM [{target_table}]")).scalar()
                        
                        if row_count == 0:
                            db.session.execute(text(f"DROP TABLE [{target_table}]"))
                        else:
                            tables_created += 1
                            yield json.dumps({'type': 'log', 'message': f"Found {row_count} errors in {clean_name}"}) + '\n'

                    except Exception as sql_ex:
                         yield json.dumps({'type': 'error', 'message': f"Failed {clean_name}: {str(sql_ex)}"}) + '\n'
                    
                    progress = int((count / total) * 100)
                    yield json.dumps({'type': 'progress', 'percent': progress}) + '\n'

            yield json.dumps({'type': 'complete', 'message': f'Scan Finished. {tables_created} error tables generated.'}) + '\n'

        except Exception as e:
             yield json.dumps({'type': 'error', 'message': format_error(e)}) + '\n'

    return Response(stream_with_context(generate_scan_stream()), mimetype='application/json')

@edata_errors_bp.route('/api/tools/edata-errors/get-defaults/<int:county_id>', methods=['GET'])
@login_required
def get_edata_defaults(county_id):
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False})
        s = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
        
        path = os.path.join(current_app.root_path, 'data', secure_filename(s.state_name), secure_filename(c.county_name), 'Images')
        book_start, book_end = "", ""
        if os.path.exists(path):
            folders = sorted([f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))])
            if folders:
                book_start, book_end = folders[0], folders[-1]
        
        townships_str = ""
        try:
            table_name = f"{c.county_name}_keli_township_ranges"
            sql = f"SELECT Township, Range FROM [{table_name}] WHERE Active = 1"
            rows = db.session.execute(text(sql)).fetchall()
            if rows:
                t_list = [f"'{r[0]},{r[1]}'" for r in rows if r[0] and r[1]]
                townships_str = ", ".join(t_list)
        except Exception:
            pass

        return jsonify({'success': True, 'book_start': book_start, 'book_end': book_end, 'townships': townships_str})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
# [GSI_END: edata_errors_api]