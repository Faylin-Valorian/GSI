import os
import glob
import re
import io
import unicodedata
from flask import Blueprint, jsonify, request, send_file, current_app, make_response
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from models import UnindexedImages
from utils import format_error

try:
    from PIL import Image
except ImportError:
    Image = None

unindexed_bp = Blueprint('unindexed', __name__)

def normalize_page_name(filename, use_split_logic):
    if not filename: return ""
    clean = filename.upper().strip()
    name_no_ext = os.path.splitext(clean)[0]
    if name_no_ext.endswith('.TIF'): name_no_ext = name_no_ext[:-4]
    if name_no_ext.endswith('.TIFF'): name_no_ext = name_no_ext[:-5]
    if not use_split_logic: return name_no_ext
    normalized_name = re.sub(r'([-_\s])0+(\d+)$', r'\1\2', name_no_ext)
    return normalized_name

@unindexed_bp.route('/api/edata/scan-unindexed', methods=['POST'])
@login_required
def scan_unindexed_images():
    try:
        county_id = request.json.get('county_id')
        scan_path = request.json.get('scan_path')
        check_split = request.json.get('check_split', False)
        debug_mode = request.json.get('debug', False)
        
        if not scan_path: return jsonify({'success': False, 'message': 'Path is required.'})
        if not os.path.isabs(scan_path): scan_path = os.path.join(current_app.root_path, scan_path)
        scan_path = os.path.abspath(scan_path)
        
        if not os.path.exists(scan_path):
            msg = f'Directory not found: {scan_path}' if debug_mode else 'Directory not found.'
            return jsonify({'success': False, 'message': msg})

        db.session.query(UnindexedImages).filter_by(county_id=county_id).delete()
        
        tif_files = glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF]'), recursive=True)
        tif_files += glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF][fF]'), recursive=True)
        tif_files = list(set(tif_files))
        
        new_entries = []
        files_by_book = {}
        for f in tif_files:
            d, n = os.path.split(f)
            b = os.path.basename(d)
            if b not in files_by_book: files_by_book[b] = []
            files_by_book[b].append((n, f))

        for book_name, file_list in files_by_book.items():
            book_param = f"{book_name}%"
            sql = text("SELECT col03varchar FROM GenericDataImport WHERE fn LIKE '%images%' AND col03varchar LIKE :pat")
            db_rows = db.session.execute(sql, {'pat': book_param}).fetchall()
            db_keys = set()
            
            for row in db_rows:
                raw = row[0]
                if not raw: continue
                parts = re.split(r'[\\/|]', raw)
                extracted_name = parts[-1].strip() if len(parts) > 1 else raw.strip()
                
                # Fallback clean
                if extracted_name.upper().startswith(book_name.upper()):
                     extracted_name = re.sub(r'^[-_ ]+', '', extracted_name[len(book_name):].strip())

                final_key = normalize_page_name(extracted_name, check_split)
                db_keys.add(final_key)

            for fname, fpath in file_list:
                file_key = normalize_page_name(fname, check_split)
                if file_key not in db_keys:
                    new_entries.append(UnindexedImages(county_id=county_id, full_path=fpath, book_name=book_name, page_name=fname, require_indexing=False))

        if new_entries:
            db.session.add_all(new_entries)
            db.session.commit()
            
        return jsonify({'success': True, 'count': len(new_entries), 'message': f'Found {len(new_entries)} unindexed images.'})

    except Exception as e:
        return jsonify({'success': False, 'message': f"Error: {str(e)}"})

@unindexed_bp.route('/api/edata/unindexed-list/<int:county_id>', methods=['GET'])
@login_required
def get_unindexed_list(county_id):
    try:
        imgs = UnindexedImages.query.filter_by(county_id=county_id).order_by(UnindexedImages.book_name, UnindexedImages.page_name).all()
        return jsonify([{
            'id': i.id,
            'path': i.full_path,
            'display_name': f"{i.book_name}\\{i.page_name}",
            'require_indexing': i.require_indexing
        } for i in imgs])
    except: return jsonify([])

@unindexed_bp.route('/api/edata/unindexed-image-data', methods=['POST'])
@login_required
def get_unindexed_image_data():
    """Adapter endpoint for ImageManager"""
    img_id = request.json.get('record_id')
    img = db.session.get(UnindexedImages, img_id)
    if not img: return jsonify({'success': False, 'message': 'Not found'})
    
    return jsonify({
        'success': True,
        'images': [{
            'src': f"/api/edata/view-image/{img.id}",
            'name': img.page_name
        }]
    })

@unindexed_bp.route('/api/edata/update-image-status', methods=['POST'])
@login_required
def update_image_status():
    try:
        img_id = request.json.get('id')
        status = request.json.get('require_indexing')
        img = db.session.get(UnindexedImages, img_id)
        if img:
            img.require_indexing = bool(status)
            db.session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'message': 'Not found'})
    except Exception as e: return jsonify({'success': False, 'message': str(e)})

@unindexed_bp.route('/api/edata/view-image/<int:id>', methods=['GET'])
@login_required
def view_local_image(id):
    try:
        if not Image: return "Server missing Image library", 500
        img = db.session.get(UnindexedImages, id)
        if not img: return "Image record not found.", 404

        file_path = os.path.abspath(img.full_path)
        if not os.path.exists(file_path): return "Image file not found on server disk.", 404

        with Image.open(file_path) as image:
            image.seek(0)
            if image.mode in ('P', 'CMYK', 'RGBA', 'LA', 'I', 'I;16', '1'):
                image = image.convert('RGB')
            img_io = io.BytesIO()
            image.save(img_io, 'JPEG', quality=85)
            img_io.seek(0)
            return make_response(send_file(img_io, mimetype='image/jpeg'))

    except Exception as e: return f"Server Error: {str(e)}", 500