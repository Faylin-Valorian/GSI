import os
import csv
import json
import time
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties, IndexingStates
from utils import format_error
from werkzeug.utils import secure_filename

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
        select '"SeriesDate"' as strng, 0 as ord 
        union select '"' + left(col06varchar,4) + '"' as strng, 1 as ord 
        from GenericDataImport 
        where fn like '%header%' and deleteFlag = 'FALSE' and record_series_external_id = '' and left(col06varchar,4) not in
            (select name from fromkellprorecord_series)
        order by ord
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

    "legalIncorrectAdditions.csv": """
        where fn like '%legal%' and deleteFlag = 'FALSE' and col05varchar != '' and addition_internal_id = '' and addition_external_id = ''
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
# [GSI_END: edata_errors_utils]

@edata_errors_bp.route('/api/tools/edata-errors/preview', methods=['POST'])
@login_required
def preview_edata_errors():
    # [GSI_BLOCK: edata_errors_preview]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    book_start = data.get('book_start', '000000')
    book_end = data.get('book_end', '999999')
    townships = data.get('townships', '')
    is_split_mode = data.get('split_images', False)
    
    # DEBUG CHECK: Hide SQL if debug is OFF
    if not data.get('debug'):
        return jsonify({'success': True, 'sql': '-- Preview Hidden. Enable Debug Mode to view SQL generation.'})

    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})

    formatted_townships = parse_townships(townships)
    
    full_script = f"-- PREVIEW: eData Errors for {c.county_name}\n"
    full_script += f"-- Book Range: {book_start} to {book_end}\n"
    full_script += f"-- Split Mode: {is_split_mode}\n\n"

    for filename, partial_sql in QUERIES.items():
        if is_split_mode and filename in SPLIT_OVERRIDES:
            partial_sql = SPLIT_OVERRIDES[filename]
            full_script += f"-- [SPLIT OVERRIDE] {filename}\n"
        else:
            full_script += f"-- {filename}\n"
            
        current_sql = partial_sql.strip()
        if not current_sql.lower().startswith('select'):
            current_sql = BASE_SQL + current_sql
        
        # Inject Params
        if '{0}' in current_sql and '{1}' in current_sql:
            current_sql = current_sql.replace('{0}', str(book_start)).replace('{1}', str(book_end))
        elif '{0}' in current_sql:
            current_sql = current_sql.replace('{0}', formatted_townships)
            
        # Renaming
        current_sql = current_sql.replace('fromkellprorecord_series', f"{c.county_name}_keli_record_series")
        current_sql = current_sql.replace('fromkellproinstrument_types', f"{c.county_name}_keli_instrument_types")
        current_sql = current_sql.replace('fromkellproadditions', f"{c.county_name}_keli_additions")
        current_sql = current_sql.replace('fromkellprocombined_manifest', f"{c.county_name}_keli_combined_manifest")

        full_script += current_sql + "\n\n"

    return jsonify({'success': True, 'sql': full_script})
    # [GSI_END: edata_errors_preview]

@edata_errors_bp.route('/api/tools/edata-errors/execute', methods=['POST'])
@login_required
def execute_edata_errors():
    # [GSI_BLOCK: edata_errors_execute]
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    data = request.json
    county_id = data.get('county_id')
    book_start = data.get('book_start')
    book_end = data.get('book_end')
    townships = data.get('townships') 
    is_split_mode = data.get('split_images', False)
    
    c = db.session.get(IndexingCounties, county_id)
    if not c: return jsonify({'success': False, 'message': 'County not found'})

    state_obj = IndexingStates.query.filter_by(fips_code=c.state_fips).first()
    if not state_obj: return jsonify({'success': False, 'message': 'State configuration error.'})

    base_path = os.path.join(current_app.root_path, 'data', secure_filename(state_obj.state_name), secure_filename(c.county_name), 'eData Errors')
    if not os.path.exists(base_path): os.makedirs(base_path)

    formatted_townships = parse_townships(townships)
    total_queries = len(QUERIES)

    def generate_stream():
        yield json.dumps({'type': 'start', 'message': f'Starting eData Error Scan ({c.county_name})...'}) + '\n'
        
        try:
            processed_count = 0
            files_created = 0
            
            with db.session.begin(): # Transaction
                for filename, partial_sql in QUERIES.items():
                    processed_count += 1
                    
                    if is_split_mode and filename in SPLIT_OVERRIDES:
                        partial_sql = SPLIT_OVERRIDES[filename]
                        yield json.dumps({'type': 'log', 'message': f"Using Split Image logic for {filename}"}) + '\n'

                    current_sql = partial_sql.strip()
                    if not current_sql.lower().startswith('select'):
                        current_sql = BASE_SQL + current_sql
                    
                    if '{0}' in current_sql and '{1}' in current_sql:
                         if not book_start or not book_end:
                             yield json.dumps({'type': 'log', 'message': f"Skipping {filename}: Missing Book Range"}) + '\n'
                             continue
                         current_sql = current_sql.replace('{0}', str(book_start)).replace('{1}', str(book_end))
                    elif '{0}' in current_sql:
                        current_sql = current_sql.replace('{0}', formatted_townships)

                    current_sql = current_sql.replace('fromkellprorecord_series', f"{c.county_name}_keli_record_series")
                    current_sql = current_sql.replace('fromkellproinstrument_types', f"{c.county_name}_keli_instrument_types")
                    current_sql = current_sql.replace('fromkellproadditions', f"{c.county_name}_keli_additions")
                    current_sql = current_sql.replace('fromkellprocombined_manifest', f"{c.county_name}_keli_combined_manifest")

                    try:
                        result = db.session.execute(text(current_sql))
                        rows = result.fetchall()
                        
                        if len(rows) > 1:
                            file_path = os.path.join(base_path, filename)
                            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                                for row in rows:
                                    f.write(str(row[0]) + '\n')
                            files_created += 1
                        
                        percent = int((processed_count / total_queries) * 100)
                        yield json.dumps({'type': 'progress', 'percent': percent, 'message': f"Processing {filename}..."}) + '\n'
                        time.sleep(0.05)

                    except Exception as ex:
                         yield json.dumps({'type': 'error', 'message': f"Error in {filename}: {str(ex)}"}) + '\n'

            yield json.dumps({'type': 'complete', 'message': f'Scan Complete. {files_created} error files generated.'}) + '\n'

        except Exception as e:
            yield json.dumps({'type': 'error', 'message': format_error(e)}) + '\n'

    return Response(stream_with_context(generate_stream()), mimetype='application/json')
    # [GSI_END: edata_errors_execute]

@edata_errors_bp.route('/api/tools/edata-errors/get-defaults/<int:county_id>', methods=['GET'])
@login_required
def get_edata_defaults(county_id):
    # [GSI_BLOCK: edata_errors_defaults]
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
    # [GSI_END: edata_errors_defaults]