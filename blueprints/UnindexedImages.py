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
    """
    Standardizes a filename for comparison.
    1. Strip whitespace & uppercase.
    2. STRIP EXTENSION (.tif, .jpg).
    3. (Optional) Remove leading zeros from suffix.
    """
    if not filename: return ""
    
    # 1. Base Clean
    clean = filename.upper().strip()
    
    # 2. Aggressive Extension Removal
    # Handles .TIF, .TIFF, .JPG, etc.
    name_no_ext = os.path.splitext(clean)[0]
    
    # Failsafe for double extensions or weird casing not caught by splitext
    if name_no_ext.endswith('.TIF'): name_no_ext = name_no_ext[:-4]
    if name_no_ext.endswith('.TIFF'): name_no_ext = name_no_ext[:-5]

    if not use_split_logic:
        return name_no_ext

    # 3. Split Logic: "0015-001" -> "0015-1"
    # Regex: (Separator)(Zeros)(Digits) -> \1\2
    normalized_name = re.sub(r'([-_\s])0+(\d+)$', r'\1\2', name_no_ext)
    
    return normalized_name

@unindexed_bp.route('/api/edata/scan-unindexed', methods=['POST'])
@login_required
def scan_unindexed_images():
    try:
        county_id = request.json.get('county_id')
        scan_path = request.json.get('scan_path')
        check_split = request.json.get('check_split', False)
        debug_mode = request.json.get('debug', False)  # <--- DEBUG TOGGLE
        
        if not scan_path:
            return jsonify({'success': False, 'message': 'Path is required.'})

        if not os.path.isabs(scan_path):
            scan_path = os.path.join(current_app.root_path, scan_path)
        scan_path = os.path.abspath(scan_path)
        
        if not os.path.exists(scan_path):
            msg = f'Directory not found: {scan_path}' if debug_mode else 'Directory not found.'
            return jsonify({'success': False, 'message': msg})

        # Clear previous results
        db.session.query(UnindexedImages).filter_by(county_id=county_id).delete()
        
        # Scan Files
        tif_files = glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF]'), recursive=True)
        tif_files += glob.glob(os.path.join(scan_path, '**', '*.[tT][iI][fF][fF]'), recursive=True)
        tif_files = list(set(tif_files))
        
        new_entries = []
        
        # Group Files by Book
        files_by_book = {}
        for f in tif_files:
            d, n = os.path.split(f)
            b = os.path.basename(d)
            if b not in files_by_book: files_by_book[b] = []
            files_by_book[b].append((n, f))

        if debug_mode:
            print(f"\n=== START SCAN ({len(files_by_book)} Books) ===")

        # Process each Book
        for book_name, file_list in files_by_book.items():
            
            # DB Query: BookName%
            book_param = f"{book_name}%"
            
            if debug_mode:
                print(f"\n--- BOOK: {book_name} ---")

            # --- TARGETING col03varchar ---
            sql = text("SELECT col03varchar FROM GenericDataImport WHERE fn LIKE '%images%' AND col03varchar LIKE :pat")
            db_rows = db.session.execute(sql, {'pat': book_param}).fetchall()
            
            db_keys = set()
            
            # --- PROCESS DB RECORDS ---
            for i, row in enumerate(db_rows):
                raw = row[0]
                if not raw: continue
                
                # Split Logic to isolate Filename from "Book | Page"
                parts = re.split(r'[\\/|]', raw)
                
                extracted_name = ""
                if len(parts) > 1:
                    extracted_name = parts[-1].strip()
                else:
                    # Fallback: Remove Bookname prefix if stuck together
                    temp = raw.strip()
                    if temp.upper().startswith(book_name.upper()):
                        rem = temp[len(book_name):].strip()
                        extracted_name = re.sub(r'^[-_ ]+', '', rem)
                    else:
                        extracted_name = temp

                # Normalize (Strip extension, standard case)
                final_key = normalize_page_name(extracted_name, check_split)
                db_keys.add(final_key)
                
                if debug_mode and i < 3:
                    print(f"  [DB col03] Raw: '{raw}' -> Key: '{final_key}'")

            # --- COMPARE FILES ---
            for i, (fname, fpath) in enumerate(file_list):
                # Normalize Local File (Strip extension)
                file_key = normalize_page_name(fname, check_split)
                
                is_match = file_key in db_keys
                
                if debug_mode and not is_match and i < 3:
                     print(f"  [MISMATCH] File: '{fname}' -> Key: '{file_key}' NOT IN DB")

                if not is_match:
                    new_entries.append(UnindexedImages(
                        county_id=county_id,
                        full_path=fpath,
                        book_name=book_name,
                        page_name=fname,
                        require_indexing=False
                    ))

        if new_entries:
            db.session.add_all(new_entries)
            db.session.commit()
            
        if debug_mode:
            print(f"\n=== COMPLETE: {len(new_entries)} unindexed ===\n")

        return jsonify({'success': True, 'count': len(new_entries), 'message': f'Found {len(new_entries)} unindexed images.'})

    except Exception as e:
        msg = f"Error: {str(e)}" if debug_mode else "Scan failed."
        if debug_mode: print(f"CRITICAL: {e}")
        return jsonify({'success': False, 'message': msg})

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
    except:
        return jsonify([])

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
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# --- VIEW IMAGE (DIAGNOSTIC & ROBUST) ---
@unindexed_bp.route('/api/edata/view-image/<int:id>', methods=['GET'])
@login_required
def view_local_image(id):
    try:
        debug_mode = request.args.get('debug', 'false').lower() == 'true'  # <--- DEBUG TOGGLE

        # 1. Check if Pillow is even installed/loaded
        if not Image:
            if debug_mode: print("CRITICAL: Pillow library not loaded. Cannot convert TIFs.")
            return "Server missing Image library", 500

        # 2. Get Record
        img = db.session.get(UnindexedImages, id)
        if not img:
            return "Image record not found.", 404

        # 3. Resolve Absolute Path
        file_path = os.path.abspath(img.full_path)
        
        if debug_mode:
            print(f"REQUEST VIEW: {file_path}")

        if not os.path.exists(file_path):
            if debug_mode: print("  -> ERROR: File missing on disk.")
            return "Image file not found on server disk.", 404

        # 4. Attempt Conversion
        try:
            with Image.open(file_path) as image:
                if debug_mode:
                    print(f"  -> Opened Successfully. Format: {image.format}, Mode: {image.mode}, Size: {image.size}")

                # HANDLE MULTIPAGE TIF (Grab Page 1)
                image.seek(0)

                # HANDLE COLOR MODES
                if image.mode in ('P', 'CMYK', 'RGBA', 'LA', 'I', 'I;16', '1'):
                    image = image.convert('RGB')
                
                # Save to RAM Buffer as JPEG
                img_io = io.BytesIO()
                image.save(img_io, 'JPEG', quality=85)
                img_io.seek(0)
                
                # Send Data
                response = make_response(send_file(img_io, mimetype='image/jpeg'))
                response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                return response

        except Exception as pil_error:
            if debug_mode: print(f"  -> PILLOW ERROR: {pil_error}")
            
            # Fallback: Try to read raw bytes to prove file access works
            try:
                with open(file_path, 'rb') as f:
                    raw_data = f.read(10)
                    if debug_mode: print(f"  -> File Read Check: OK (Header: {raw_data})")
                return f"Conversion Failed: {str(pil_error)}", 500
            except Exception as io_err:
                if debug_mode: print(f"  -> IO ERROR: {io_err}")
                return f"File Access Error: {str(io_err)}", 500

    except Exception as e:
        msg = f"Server Error: {str(e)}"
        if debug_mode: print(f"VIEW ERROR: {msg}")
        return msg, 500