import os
import sys
import time
import json
import zipfile
import threading
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app, session
from flask_login import login_required, current_user

patch_bp = Blueprint('patch_manager', __name__)

VERSION_FILE = 'version.json'
BACKUP_DIR = 'versions'

# --- VERSIONING UTILS ---
def load_version(root):
    v_path = os.path.join(root, VERSION_FILE)
    if not os.path.exists(v_path):
        # Default starting version
        return {"major": 0, "minor": 0, "patch": 1, "string": "0.0.1"}
    with open(v_path, 'r') as f:
        return json.load(f)

def increment_version(root):
    v = load_version(root)
    
    # Increment Patch
    v['patch'] += 1
    
    # Rollover Logic: 99 -> 0, increment minor
    if v['patch'] >= 100:
        v['patch'] = 0
        v['minor'] += 1
        
    # Rollover Logic: 99 -> 0, increment major
    if v['minor'] >= 100:
        v['minor'] = 0
        v['major'] += 1

    v['string'] = f"{v['major']}.{v['minor']}.{v['patch']}"
    
    # Save back to file
    with open(os.path.join(root, VERSION_FILE), 'w') as f:
        json.dump(v, f, indent=4)
        
    return v['string']

# --- BACKUP UTILS ---
def create_backup(root, current_ver):
    """
    Zips the entire application folder into /versions/YYYYMMDD_vX.X.X.zip
    Excludes: versions, data, .git, __pycache__, instance, venv, .idea
    """
    backup_folder = os.path.join(root, BACKUP_DIR)
    os.makedirs(backup_folder, exist_ok=True)
    
    date_str = datetime.now().strftime('%Y%m%d')
    zip_name = f"{date_str}_v{current_ver}.zip"
    zip_path = os.path.join(backup_folder, zip_name)
    
    print(f" >>> STARTING BACKUP: {zip_path}")
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for foldername, subfolders, filenames in os.walk(root):
                # Filter out excluded folders
                # We modify 'subfolders' in-place to prevent walking into them
                # ADDED 'data' to the exclusion list here
                subfolders[:] = [d for d in subfolders if d not in [BACKUP_DIR, 'data', '.git', '__pycache__', 'instance', 'venv', '.idea']]

                for filename in filenames:
                    if filename == zip_name: continue # Don't backup the file we are writing
                    if filename.endswith('.pyc') or filename.endswith('.log'): continue
                    
                    file_path = os.path.join(foldername, filename)
                    rel_path = os.path.relpath(file_path, root)
                    zipf.write(file_path, rel_path)
        return zip_name
    except Exception as e:
        print(f" !!! BACKUP FAILED: {str(e)}")
        return None

# --- RESTART UTILS ---
def restart_server():
    """Restarts the Flask application process."""
    print(" >>> SYSTEM UPDATE COMPLETE. RESTARTING SERVICE...")
    time.sleep(2) # Give the request time to return response
    os.execv(sys.executable, [sys.executable] + sys.argv)

# --- PATCHER LOGIC ---
class SimplePatcher:
    """Parses and applies standard .diff files."""
    def apply(self, patch_content, root_path):
        lines = patch_content.splitlines()
        hunks = self.parse(lines)
        results = []
        
        for hunk in hunks:
            full_path = os.path.join(root_path, hunk['file'])
            
            # Skip if file doesn't exist (unless we add file creation logic later)
            if not os.path.exists(full_path):
                results.append(f"Skipped {hunk['file']} (File not found)")
                continue
            
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # Try Exact Match
                if hunk['search'] in content:
                    new_content = content.replace(hunk['search'], hunk['replace'], 1)
                    with open(full_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    results.append(f"Patched {hunk['file']}")
                else:
                    # Try Normalized Line Endings (CRLF fix)
                    norm_content = content.replace('\r\n', '\n')
                    if hunk['search'] in norm_content:
                        new_content = norm_content.replace(hunk['search'], hunk['replace'], 1)
                        with open(full_path, 'w', encoding='utf-8') as f:
                            f.write(new_content)
                        results.append(f"Patched {hunk['file']} (Normalized)")
                    else:
                        results.append(f"Failed to match context in {hunk['file']}")
            except Exception as e:
                results.append(f"Error {hunk['file']}: {str(e)}")
                
        return results

    def parse(self, lines):
        hunks = []
        current_file = None
        i = 0
        while i < len(lines):
            line = lines[i]
            # File Headers
            if line.startswith('+++ b/'):
                current_file = line[6:].strip()
                i += 1; continue
            elif line.startswith('--- a/') or line.startswith('index '):
                i += 1; continue
            
            # Hunks
            if line.startswith('@@'):
                i += 1
                search, replace = [], []
                while i < len(lines):
                    hl = lines[i]
                    if hl.startswith('diff ') or hl.startswith('--- ') or hl.startswith('+++ '): break
                    
                    if hl.startswith(' '):
                        search.append(hl[1:])
                        replace.append(hl[1:])
                    elif hl.startswith('-'):
                        search.append(hl[1:])
                    elif hl.startswith('+'):
                        replace.append(hl[1:])
                    i += 1
                if current_file:
                    hunks.append({'file': current_file, 'search': "\n".join(search), 'replace': "\n".join(replace)})
                continue
            i += 1
        return hunks

# --- API ROUTES ---

@patch_bp.route('/api/admin/version', methods=['GET'])
@login_required
def get_version():
    v = load_version(current_app.root_path)
    return jsonify({'version': v['string']})

@patch_bp.route('/api/admin/patch/apply', methods=['POST'])
@login_required
def apply_patch():
    # 1. SECURITY CHECKS
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
    
    if not session.get('debug_mode'):
        return jsonify({'success': False, 'message': 'Debug Mode must be enabled.'}), 403

    patch_content = ""

    # Determine Source (File vs Text)
    # Priority to text content if provided
    if 'patch_content' in request.form and request.form['patch_content'].strip():
        patch_content = request.form['patch_content']
        
        # Save manual patch for reversion/history
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"manual_patch_{timestamp}.diff"
        patches_dir = os.path.join(current_app.root_path, 'patches')
        try:
            os.makedirs(patches_dir, exist_ok=True)
            with open(os.path.join(patches_dir, filename), 'w', encoding='utf-8') as f:
                f.write(patch_content)
        except Exception as e:
            print(f"Warning: Could not save manual patch backup: {e}")

    elif 'file' in request.files:
        file = request.files['file']
        if not file.filename.endswith('.diff'):
            return jsonify({'success': False, 'message': 'Invalid file format. .diff required.'}), 400
        patch_content = file.read().decode('utf-8')
    else:
        return jsonify({'success': False, 'message': 'No patch data provided'}), 400

    try:
        root = current_app.root_path
        
        # 2. LOAD VERSION & BACKUP
        current_v = load_version(root)['string']
        backup_name = create_backup(root, current_v)
        
        if not backup_name:
            return jsonify({'success': False, 'message': 'Backup failed. Update aborted.'}), 500

        # 3. APPLY PATCH
        # Normalize line endings
        patch_content = patch_content.replace('\r\n', '\n')
        patcher = SimplePatcher()
        logs = patcher.apply(patch_content, root)
        
        # 4. INCREMENT VERSION
        new_v = increment_version(root)
        
        # 5. PREPARE LOGS
        log_text = f"BACKUP: {backup_name}\nVERSION: {current_v} -> {new_v}\n--------------------------\n" + "\n".join(logs)
        print(f"\n >>> PATCH REPORT:\n{log_text}\n")

        # 6. TRIGGER RESTART
        threading.Thread(target=restart_server).start()

        return jsonify({
            'success': True, 
            'message': f"System patched to v{new_v}. Restarting...",
            'logs': log_text
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500