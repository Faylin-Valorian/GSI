import os
import glob
import re
import io
from flask import Blueprint, jsonify, request, send_file, current_app, make_response
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import IndexingCounties
from utils import format_error

try:
    from PIL import Image
except ImportError:
    Image = None

unindexed_bp = Blueprint('unindexed', __name__)

def get_unindexed_table(county_name):
    """Returns the name of the dynamic unindexed images table."""
    return f"{county_name}_unindexed_images"

def normalize_page_name(filename):
    """
    Standardizes a filename for comparison: Strip whitespace, uppercase, 
    and remove extensions (.tif, .tiff, .jpg, etc.).
    """
    if not filename: return ""
    clean = filename.upper().strip()
    name_no_ext = os.path.splitext(clean)[0]
    
    # Failsafe for double extensions
    if name_no_ext.endswith('.TIF'): name_no_ext = name_no_ext[:-4]
    if name_no_ext.endswith('.TIFF'): name_no_ext = name_no_ext[:-5]
    return name_no_ext

def normalize_path_for_comparison(path_str):
    """Standardizes path to start from the 'data' folder for DB comparison."""
    if not path_str: return None
    parts = re.split(r'[/\\]', path_str.strip())
    start_index = -1
    for i, p in enumerate(parts):
        if p.lower() == 'data':
            start_index = i
            break
    if start_index != -1:
        return "/".join(p.lower() for p in parts[start_index:])
    return "/".join(p.lower() for p in parts)

@unindexed_bp.route('/api/edata/scan-unindexed', methods=['POST'])
@login_required
def scan_unindexed_images():
    try:
        county_id = request.json.get('county_id')
        scan_path = request.json.get('scan_path')
        debug_mode = request.json.get('debug', False)
        
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify({'success': False, 'message': 'County not found'})
        
        target_table = get_unindexed_table(c.county_name)
        if not scan_path: return jsonify({'success': False, 'message': 'Path is required.'})
        
        if not os.path.isabs(scan_path):
            scan_path = os.path.join(current_app.root_path, scan_path)
        scan_path = os.path.abspath(scan_path)
        
        if not os.path.exists(scan_path):
            return jsonify({'success': False, 'message': 'Directory not found.'})

        # 1. Ensure dynamic table exists
        sql_create = f"""
        IF OBJECT_ID('[{target_table}]', 'U') IS NULL
        CREATE TABLE [{target_table}] (
            id INT IDENTITY(1,1) PRIMARY KEY,
            full_path NVARCHAR(MAX),
            book_name NVARCHAR(255),
            page_name NVARCHAR(255),
            require_indexing BIT DEFAULT 0
        )
        """
        db.session.execute(text(sql_create))

        # 2. Fetch known paths from DB to exclude
        sql_known = "SELECT stech_image_path FROM GenericDataImport WHERE fn LIKE '%image%' AND stech_image_path IS NOT NULL"
        known_rows = db.session.execute(text(sql_known)).fetchall()
        
        known_paths = set()
        for r in known_rows:
            norm = normalize_path_for_comparison(r[0])
            if norm: known_paths.add(norm)

        # 3. Scan disk for TIFF files
        tif_files = glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF]'), recursive=True)
        tif_files += glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF][fF]'), recursive=True)
        tif_files = list(set(tif_files))
        
        new_entries = []
        db.session.execute(text(f"TRUNCATE TABLE [{target_table}]"))
        
        for fpath in tif_files:
            disk_norm = normalize_path_for_comparison(fpath)
            
            # If disk file is not in GenericDataImport, it's unindexed
            if disk_norm not in known_paths:
                dname, fname = os.path.split(fpath)
                book = os.path.basename(dname)
                
                safe_path = fpath.replace("'", "''")
                safe_book = book.replace("'", "''")
                safe_page = fname.replace("'", "''")
                new_entries.append(f"('{safe_path}', '{safe_book}', '{safe_page}', 0)")

        # 4. Save Results using chunked inserts
        if new_entries:
            chunk_size = 500
            for i in range(0, len(new_entries), chunk_size):
                chunk = new_entries[i:i + chunk_size]
                sql_insert = f"INSERT INTO [{target_table}] (full_path, book_name, page_name, require_indexing) VALUES {','.join(chunk)}"
                db.session.execute(text(sql_insert))
            
        db.session.commit()
        return jsonify({'success': True, 'count': len(new_entries), 'message': f'Found {len(new_entries)} unindexed images.'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': format_error(e)})

@unindexed_bp.route('/api/edata/unindexed-list/<int:county_id>', methods=['GET'])
@login_required
def get_unindexed_list(county_id):
    try:
        c = db.session.get(IndexingCounties, county_id)
        if not c: return jsonify([])
        target_table = get_unindexed_table(c.county_name)
        
        # Verify table exists before querying
        check = db.session.execute(text(f"IF OBJECT_ID('[{target_table}]', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")).fetchone()
        if not check or check[0] == 0: return jsonify([])

        sql = f"SELECT id, full_path, book_name, page_name, require_indexing FROM [{target_table}] ORDER BY book_name, page_name"
        rows = db.session.execute(text(sql)).fetchall()
        
        return jsonify([{
            'id': r.id,
            'path': r.full_path,
            'display_name': f"{r.book_name}\\{r.page_name}",
            'require_indexing': bool(r.require_indexing)
        } for r in rows])
    except: return jsonify([])

@unindexed_bp.route('/api/edata/unindexed-image-data', methods=['POST'])
@login_required
def get_unindexed_image_data():
    try:
        img_id = request.json.get('record_id')
        county_id = request.json.get('county_id')
        if not county_id: return jsonify({'success': False, 'message': 'County ID required'})
        
        c = db.session.get(IndexingCounties, county_id)
        target_table = get_unindexed_table(c.county_name)
        
        sql = f"SELECT id, page_name FROM [{target_table}] WHERE id = :id"
        row = db.session.execute(text(sql), {'id': img_id}).fetchone()
        
        if not row: return jsonify({'success': False, 'message': 'Not found'})
        
        return jsonify({
            'success': True,
            'images': [{
                'src': f"/api/edata/view-image/{row.id}?cid={county_id}",
                'name': row.page_name
            }]
        })
    except Exception as e: return jsonify({'success': False, 'message': str(e)})

@unindexed_bp.route('/api/edata/update-image-status', methods=['POST'])
@login_required
def update_image_status():
    try:
        data = request.json
        img_id = data.get('id')
        status = 1 if data.get('require_indexing') else 0
        county_id = data.get('county_id')
        
        c = db.session.get(IndexingCounties, county_id)
        target_table = get_unindexed_table(c.county_name)
        
        sql = f"UPDATE [{target_table}] SET require_indexing = :status WHERE id = :id"
        db.session.execute(text(sql), {'status': status, 'id': img_id})
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})

@unindexed_bp.route('/api/edata/view-image/<int:id>', methods=['GET'])
@login_required
def view_local_image(id):
    """Diagnostically views a local image from the dynamic table."""
    try:
        if not Image: return "Server missing Image library", 500
        county_id = request.args.get('cid')
        if not county_id: return "Context missing", 400
        
        c = db.session.get(IndexingCounties, county_id)
        target_table = get_unindexed_table(c.county_name)
        
        sql = f"SELECT full_path FROM [{target_table}] WHERE id = :id"
        row = db.session.execute(text(sql), {'id': id}).fetchone()
        if not row: return "Image record not found.", 404

        file_path = row.full_path
        if not os.path.exists(file_path): return "Image file not found on server.", 404

        with Image.open(file_path) as image:
            image.seek(0)
            # Handle standard Pillow conversions for display
            if image.mode in ('P', 'CMYK', 'RGBA', 'LA', 'I', 'I;16', '1'):
                image = image.convert('RGB')
            img_io = io.BytesIO()
            image.save(img_io, 'JPEG', quality=85)
            img_io.seek(0)
            return make_response(send_file(img_io, mimetype='image/jpeg'))
    except Exception as e: return f"Server Error: {str(e)}", 500