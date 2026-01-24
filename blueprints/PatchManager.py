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
PATCH_STORAGE_DIR = 'patches'

# --- UTILITY FUNCTIONS ---

def load_version(root):
    """Loads the current system version."""
    v_path = os.path.join(root, VERSION_FILE)
    if not os.path.exists(v_path):
        return {"major": 0, "minor": 0, "patch": 1, "string": "0.0.1"}
    try:
        with open(v_path, 'r') as f:
            return json.load(f)
    except:
        return {"major": 0, "minor": 0, "patch": 1, "string": "0.0.1"}

def increment_version(root):
    """Increments the patch version number."""
    v = load_version(root)
    v['patch'] += 1
    if v['patch'] >= 100:
        v['patch'] = 0
        v['minor'] += 1
    if v['minor'] >= 100:
        v['minor'] = 0
        v['major'] += 1
    
    v['string'] = f"{v['major']}.{v['minor']}.{v['patch']}"
    
    with open(os.path.join(root, VERSION_FILE), 'w') as f:
        json.dump(v, f, indent=4)
    
    return v['string']

def create_backup(root, current_ver):
    """Creates a full zip backup of the application code."""
    backup_folder = os.path.join(root, BACKUP_DIR)
    os.makedirs(backup_folder, exist_ok=True)
    
    date_str = datetime.now().strftime('%Y%m%d')
    zip_name = f"{date_str}_v{current_ver}.zip"
    zip_path = os.path.join(backup_folder, zip_name)
    
    print(f" >>> STARTING BACKUP: {zip_path}")
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for foldername, subfolders, filenames in os.walk(root):
                # Exclude backup folders, git, and virtual environments
                subfolders[:] = [d for d in subfolders if d not in [BACKUP_DIR, PATCH_STORAGE_DIR, 'data', '.git', '__pycache__', 'instance', 'venv', '.idea']]

                for filename in filenames:
                    if filename == zip_name: continue
                    if filename.endswith('.pyc') or filename.endswith('.log'): continue
                    
                    file_path = os.path.join(foldername, filename)
                    rel_path = os.path.relpath(file_path, root)
                    zipf.write(file_path, rel_path)
        return zip_name
    except Exception as e:
        print(f" !!! BACKUP FAILED: {str(e)}")
        return None

def restart_server():
    """Restarts the Flask process."""
    print(" >>> SYSTEM UPDATE COMPLETE. RESTARTING...")
    
    # Allow time for the response to be sent back to the client
    time.sleep(2) 
    
    # Restart the current process
    os.execv(sys.executable, [sys.executable] + sys.argv)

# --- ATOMIC PATCHER ENGINE ---

class SimplePatcher:
    def parse(self, lines):
        """Parses a unified diff into a list of file operations."""
        hunks = []
        current_file = None
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith('+++ b/'):
                current_file = line[6:].strip()
                i += 1; continue
            elif line.startswith('--- a/') or line.startswith('index '):
                i += 1; continue
            
            if line.startswith('@@'):
                if not current_file: 
                    i += 1; continue 
                
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
                
                hunks.append({'file': current_file, 'search': "\n".join(search), 'replace': "\n".join(replace)})
                continue
            i += 1
        return hunks

    def apply(self, patch_content, root_path):
        """ATOMIC APPLY: Verifies all hunks before writing any file."""
        lines = patch_content.splitlines()
        hunks = self.parse(lines)
        if not hunks: raise Exception("No valid patch segments found.")
        
        # Group by file
        file_map = {}
        for h in hunks:
            if h['file'] not in file_map: file_map[h['file']] = []
            file_map[h['file']].append(h)
            
        pending_writes = {} 
        logs = []

        for filename, fhunks in file_map.items():
            full_path = os.path.join(root_path, filename)
            
            # Read original content
            if os.path.exists(full_path):
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            else:
                content = None # Signal that file doesn't exist

            # Apply hunks in order
            for hunk in fhunks:
                if content is None:
                    # Creating new file
                    if not hunk['search'].strip():
                        content = hunk['replace'] # New content
                        logs.append(f"Create: {filename}")
                    else:
                        raise Exception(f"File missing: {filename} (Cannot modify non-existent file)")
                else:
                    # Modifying existing
                    c_norm = content.replace('\r\n', '\n')
                    s_norm = hunk['search'].replace('\r\n', '\n')
                    
                    if s_norm in c_norm:
                        content = c_norm.replace(s_norm, hunk['replace'], 1)
                        logs.append(f"Patch: {filename}")
                    else:
                        start_snippet = c_norm[:50].replace('\n', '\\n')
                        raise Exception(f"Context mismatch in {filename}. Search block not found.\nFile starts with: {start_snippet}...")
            
            pending_writes[full_path] = content

        # Commit all changes
        for path, data in pending_writes.items():
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(data)
        
        return logs

# --- API ENDPOINTS ---

@patch_bp.route('/api/admin/version', methods=['GET'])
@login_required
def get_version():
    v = load_version(current_app.root_path)
    return jsonify({'version': v['string']})

@patch_bp.route('/api/admin/patch/apply', methods=['POST'])
@login_required
def apply_patch():
    try:
        if current_user.role != 'admin':
            return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
        patch_content = ""

        # 1. Get Patch Content
        if 'patch_content' in request.form and request.form['patch_content'].strip():
            patch_content = request.form['patch_content']
            
            # Save Manual Patch for history
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"manual_patch_{timestamp}.diff"
                patches_dir = os.path.join(current_app.root_path, PATCH_STORAGE_DIR)
                os.makedirs(patches_dir, exist_ok=True)
                with open(os.path.join(patches_dir, filename), 'w', encoding='utf-8') as f:
                    f.write(patch_content)
            except: pass

        elif 'file' in request.files:
            file = request.files['file']
            if not file.filename.endswith('.diff'):
                return jsonify({'success': False, 'message': 'Invalid file format. .diff required.'}), 400
            patch_content = file.read().decode('utf-8')
        else:
            return jsonify({'success': False, 'message': 'No patch data provided'}), 400

        root = current_app.root_path
        current_v = load_version(root)['string']
        
        # 2. Create Backup
        backup_name = create_backup(root, current_v)
        if not backup_name:
            return jsonify({'success': False, 'message': 'Backup failed. Update aborted for safety.'}), 500

        # 3. Apply Patch
        patcher = SimplePatcher()
        # Clean inputs
        patch_content = patch_content.replace('\r\n', '\n')
        
        logs = patcher.apply(patch_content, root)
        
        # 4. Increment Version & Restart
        new_v = increment_version(root)
        
        log_text = f"BACKUP: {backup_name}\nVERSION: {current_v} -> {new_v}\n--------------------------\n" + "\n".join(logs)
        print(f"\n >>> PATCH SUCCESS:\n{log_text}\n")

        # Spawn restart thread
        threading.Thread(target=restart_server).start()

        return jsonify({
            'success': True, 
            'message': f"System patched to v{new_v}. Restarting...",
            'logs': log_text
        })

    except Exception as e:
        # Log the error and return 500 so the UI knows it failed
        print(f"PATCH ERROR: {str(e)}")
        return jsonify({'success': False, 'message': f"PATCH FAILED: {str(e)}"}), 500