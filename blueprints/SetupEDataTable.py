import os
import json
from flask import Blueprint, request, Response, stream_with_context, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from werkzeug.utils import secure_filename
from extensions import db
from models import IndexingCounties, IndexingStates
from utils import format_error  # <--- IMPORTED

setup_edata_bp = Blueprint('setup_edata', __name__)

def parse_line_waterfall(line):
    """
    Replicates the logic of tvf_ParseGenericRow:
    1. Extracts 10 quoted values sequentially.
    2. Splits the remainder by comma into 20 values.
    """
    c_cols = []
    remainder = line
    
    # Step 1: Extract 10 quoted values (c1..c10)
    for _ in range(10):
        try:
            p1 = remainder.find('"')
            if p1 == -1: 
                c_cols.append('')
                continue
                
            p2 = remainder.find('"', p1 + 1)
            if p2 == -1: 
                c_cols.append('')
                continue
            
            # Extract content between quotes
            val = remainder[p1+1:p2].strip()
            c_cols.append(val)
            
            # Remove processed part (STUFF logic)
            remainder = remainder[p2+1:]
        except Exception:
            c_cols.append('')

    # Step 2: Split remainder by comma (o1..o20)
    o_raw = remainder.split(',')
    o_cols = [x.strip() for x in o_raw]
    
    # Pad or Trim to exactly 20 columns
    if len(o_cols) < 20:
        o_cols.extend([''] * (20 - len(o_cols)))
    else:
        o_cols = o_cols[:20]
        
    return c_cols, o_cols, remainder

@setup_edata_bp.route('/api/tools/setup-edata/preview', methods=['POST'])
@login_required
def preview_generic_import():
    if current_user.role != 'admin': return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    data = request.json
    county_id = data.get('county_id')
    
    # 1. Resolve Paths
    county = db.session.get(IndexingCounties, county_id)
    if not county: return jsonify({'success': False, 'message': 'County not found'})
    
    state = IndexingStates.query.filter_by(fips_code=county.state_fips).first()
    if not state: return jsonify({'success': False, 'message': 'State not found'})

    s_clean = secure_filename(state.state_name)
    c_clean = secure_filename(county.county_name)
    base_folder = os.path.join(current_app.root_path, 'data', s_clean, c_clean, 'eData Files')
    
    if not os.path.exists(base_folder):
        return jsonify({'success': False, 'message': 'eData Files folder not found.'})

    csv_files = [f for f in os.listdir(base_folder) if f.lower().endswith('.csv')]
    if not csv_files:
        return jsonify({'success': True, 'sql': '-- No .csv files found.'})

    # 2. Generate Preview (First 5 rows of first file)
    sql_preview = "-- PREVIEW: Generated Insert Statements (First 5 rows of first file)\n"
    sql_preview += f"-- Source: {csv_files[0]}\n\n"

    col_list = "FN, OriginalValue, col01varchar, col02varchar, col03varchar, col04varchar, col05varchar, col06varchar, col07varchar, col08varchar, col09varchar, col10varchar, col01other, col02other, col03other, col04other, col05other, col06other, col07other, col08other, col09other, col10other, col11other, col12other, col13other, col14other, col15other, col16other, col17other, col18other, col19other, col20other, uf1, uf2, uf3, leftovers"

    full_path = os.path.join(base_folder, csv_files[0])
    try:
        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            rows_shown = 0
            for line in f:
                if rows_shown >= 5: break
                clean_line = line.strip()
                if not clean_line: continue
                
                c_vals, o_vals, left_val = parse_line_waterfall(clean_line)
                
                # Escape and Format
                vals = [csv_files[0], clean_line[:8000]] + c_vals + o_vals + ['', '', '', left_val]
                vals_sql = ", ".join([f"'{v.replace('\'', '\'\'')}'" for v in vals])
                
                sql_preview += f"INSERT INTO [dbo].[GenericDataImport] ({col_list}) VALUES ({vals_sql});\n"
                rows_shown += 1
    except Exception as e:
        sql_preview += f"\n-- Error reading file: {str(e)}"

    return jsonify({'success': True, 'sql': sql_preview})

@setup_edata_bp.route('/api/tools/setup-edata', methods=['POST'])
@login_required
def run_generic_import():
    req_data = request.json
    user_role = current_user.role

    def generate():
        if user_role != 'admin':
            yield json.dumps({'type': 'error', 'message': 'Unauthorized'}) + '\n'
            return

        county_id = req_data.get('county_id')
        import_mode = req_data.get('mode', 'D') # D=Drop, A=Append

        if not county_id:
            yield json.dumps({'type': 'error', 'message': 'Context missing'}) + '\n'
            return

        # 1. Resolve Paths
        county = db.session.get(IndexingCounties, county_id)
        state = IndexingStates.query.filter_by(fips_code=county.state_fips).first()
        
        if not county or not state:
            yield json.dumps({'type': 'error', 'message': 'Invalid State/County'}) + '\n'
            return

        s_clean = secure_filename(state.state_name)
        c_clean = secure_filename(county.county_name)
        base_folder = os.path.join(current_app.root_path, 'data', s_clean, c_clean, 'eData Files')
        abs_folder_path = os.path.abspath(base_folder)

        if not os.path.exists(abs_folder_path):
            yield json.dumps({'type': 'error', 'message': 'eData Files folder not found.'}) + '\n'
            return

        # 2. Database Prep
        if import_mode == 'D':
            try:
                db.session.execute(text("IF OBJECT_ID('[dbo].[GenericDataImport]', 'U') IS NOT NULL DROP TABLE [dbo].[GenericDataImport]"))
                create_sql = """
                CREATE TABLE [dbo].[GenericDataImport] (
                    ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    FN VARCHAR(1000),
                    OriginalValue VARCHAR(MAX),
                    col01varchar VARCHAR(1000), col02varchar VARCHAR(1000), col03varchar VARCHAR(1000), col04varchar VARCHAR(1000), col05varchar VARCHAR(1000),
                    col06varchar VARCHAR(1000), col07varchar VARCHAR(1000), col08varchar VARCHAR(1000), col09varchar VARCHAR(1000), col10varchar VARCHAR(1000),
                    col01other VARCHAR(1000), col02other VARCHAR(1000), col03other VARCHAR(1000), col04other VARCHAR(1000), col05other VARCHAR(1000),
                    col06other VARCHAR(1000), col07other VARCHAR(1000), col08other VARCHAR(1000), col09other VARCHAR(1000), col10other VARCHAR(1000),
                    col11other VARCHAR(1000), col12other VARCHAR(1000), col13other VARCHAR(1000), col14other VARCHAR(1000), col15other VARCHAR(1000),
                    col16other VARCHAR(1000), col17other VARCHAR(1000), col18other VARCHAR(1000), col19other VARCHAR(1000), col20other VARCHAR(1000),
                    uf1 VARCHAR(1000), uf2 VARCHAR(1000), uf3 VARCHAR(1000),
                    leftovers VARCHAR(1000)
                )
                """
                db.session.execute(text(create_sql))
                db.session.commit()
            except Exception as e:
                # Use format_error here too if desired, though this is schema setup
                yield json.dumps({'type': 'error', 'message': f'Table Creation Failed: {format_error(e)}'}) + '\n'
                return

        # 3. Collect Files
        csv_files = []
        for root, dirs, files in os.walk(abs_folder_path):
            for file in files:
                if file.lower().endswith('.csv'):
                    csv_files.append(os.path.join(root, file))

        total_files = len(csv_files)
        if total_files == 0:
            yield json.dumps({'type': 'error', 'message': 'No .csv files found.'}) + '\n'
            return

        yield json.dumps({'type': 'progress', 'current': 0, 'total': total_files, 'percent': 0}) + '\n'

        processed_files = 0
        errors = []

        # 4. Processing Loop (Python Logic)
        insert_query = text("""
            INSERT INTO [dbo].[GenericDataImport] (
                FN, OriginalValue,
                col01varchar, col02varchar, col03varchar, col04varchar, col05varchar,
                col06varchar, col07varchar, col08varchar, col09varchar, col10varchar,
                col01other, col02other, col03other, col04other, col05other,
                col06other, col07other, col08other, col09other, col10other,
                col11other, col12other, col13other, col14other, col15other,
                col16other, col17other, col18other, col19other, col20other,
                uf1, uf2, uf3, leftovers
            ) VALUES (
                :fn, :orig,
                :c1, :c2, :c3, :c4, :c5, :c6, :c7, :c8, :c9, :c10,
                :o1, :o2, :o3, :o4, :o5, :o6, :o7, :o8, :o9, :o10,
                :o11, :o12, :o13, :o14, :o15, :o16, :o17, :o18, :o19, :o20,
                '', '', '', :left
            )
        """)

        for i, full_path in enumerate(csv_files):
            filename = os.path.basename(full_path)
            batch_data = []
            
            try:
                # Open with errors='replace' to prevent encoding crashes
                with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                    # Skip basic header? Usually handled by logic, but raw text often has it. 
                    # If we need to skip first row, uncomment:
                    # next(f, None) 
                    
                    for line in f:
                        clean_line = line.strip()
                        if not clean_line: continue
                        
                        # PARSE
                        c_vals, o_vals, left_val = parse_line_waterfall(clean_line)
                        
                        row_dict = {
                            'fn': filename,
                            'orig': clean_line[:8000], # Truncate safely if huge
                            'c1': c_vals[0], 'c2': c_vals[1], 'c3': c_vals[2], 'c4': c_vals[3], 'c5': c_vals[4],
                            'c6': c_vals[5], 'c7': c_vals[6], 'c8': c_vals[7], 'c9': c_vals[8], 'c10': c_vals[9],
                            'o1': o_vals[0], 'o2': o_vals[1], 'o3': o_vals[2], 'o4': o_vals[3], 'o5': o_vals[4],
                            'o6': o_vals[5], 'o7': o_vals[6], 'o8': o_vals[7], 'o9': o_vals[8], 'o10': o_vals[9],
                            'o11': o_vals[10], 'o12': o_vals[11], 'o13': o_vals[12], 'o14': o_vals[13], 'o15': o_vals[14],
                            'o16': o_vals[15], 'o17': o_vals[16], 'o18': o_vals[17], 'o19': o_vals[18], 'o20': o_vals[19],
                            'left': left_val
                        }
                        batch_data.append(row_dict)

                        # BATCH INSERT (Every 1000 rows)
                        if len(batch_data) >= 1000:
                            db.session.execute(insert_query, batch_data)
                            db.session.commit()
                            batch_data = []

                    # Insert remaining rows for this file
                    if batch_data:
                        db.session.execute(insert_query, batch_data)
                        db.session.commit()

                processed_files += 1

            except Exception as e:
                db.session.rollback()
                
                # --- NEW ERROR HANDLING ---
                error_message = format_error(e)
                errors.append(f"{filename}: {error_message}")

            # Update Progress Bar
            percent = int(((i + 1) / total_files) * 100)
            yield json.dumps({
                'type': 'progress',
                'current': i + 1,
                'total': total_files,
                'percent': percent,
                'filename': filename
            }) + '\n'

        # 5. Final Report
        if processed_files == 0 and len(errors) > 0:
            yield json.dumps({'type': 'error', 'message': f"All failed. First: {errors[0]}"}) + '\n'
        else:
            msg = f"Processed {processed_files} files."
            if errors: msg += f" ({len(errors)} errors)"
            yield json.dumps({'type': 'complete', 'message': msg}) + '\n'

    return Response(stream_with_context(generate()), mimetype='application/json')