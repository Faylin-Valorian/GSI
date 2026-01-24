import os
import sys
import time
import json
import zipfile
import threading
import re
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user

patch_bp = Blueprint('patch_manager', __name__)

VERSION_FILE = 'version.json'
BACKUP_DIR = 'versions'
PATCH_STORAGE_DIR = 'patches'

# --- UTILITY FUNCTIONS (PRESERVED) ---

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
    time.sleep(2) 
    os.execv(sys.executable, [sys.executable] + sys.argv)

# --- ROBUST PATCHER ENGINE (UPGRADED) ---

class RobustPatcher:
    def normalize_line(self, line):
        """Strip whitespace for loose comparison."""
        return "".join(line.split())

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
                    # Stop if we hit a new hunk or file header
                    if hl.startswith('diff ') or hl.startswith('--- ') or hl.startswith('+++ '): break
                    # Stop if we hit a new @@ block
                    if hl.startswith('@@'): break 
                    
                    if hl.startswith(' '):
                        search.append(hl[1:])
                        replace.append(hl[1:])
                    elif hl.startswith('-'):
                        search.append(hl[1:])
                    elif hl.startswith('+'):
                        replace.append(hl[1:])
                    elif hl == '' or hl == '\n':
                        # Handle empty lines in diffs sometimes missing space
                        search.append("")
                        replace.append("")
                    i += 1
                
                hunks.append({'file': current_file, 'search_lines': search, 'replace_lines': replace})
                continue
            i += 1
        return hunks

    def apply(self, patch_content, root_path):
        """Applies hunks using fuzzy line matching."""
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
            file_lines = []
            if os.path.exists(full_path):
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    file_lines = f.read().splitlines()
            
            # Apply hunks
            for hunk in fhunks:
                search_block = hunk['search_lines']
                replace_block = hunk['replace_lines']
                
                # Case 1: New File (Search block is empty)
                if not search_block and not file_lines:
                    file_lines = replace_block
                    logs.append(f"Create: {filename}")
                    continue

                # Case 2: Fuzzy Match
                # We try to find the search block in the file lines ignoring whitespace
                found_idx = -1
                
                # If search block is empty but file exists, it's an append or overwrite? 
                # Assuming context is usually provided. 
                if not search_block:
                     # Fallback for overwrite
                     file_lines = replace_block
                     logs.append(f"Overwrite: {filename}")
                     continue

                search_len = len(search_block)
                # Optimization: normalized search block
                norm_search = [self.normalize_line(l) for l in search_block]
                
                for idx in range(len(file_lines) - search_len + 1):
                    match = True
                    for offset in range(search_len):
                        if self.normalize_line(file_lines[idx+offset]) != norm_search[offset]:
                            match = False
                            break
                    if match:
                        found_idx = idx
                        break
                
                if found_idx != -1:
                    # Replace lines
                    # Remove old lines
                    del file_lines[found_idx:found_idx+search_len]
                    # Insert new lines
                    for r_line in reversed(replace_block):
                        file_lines.insert(found_idx, r_line)
                    logs.append(f"Patch: {filename} (Block found at line {found_idx+1})")
                else:
                    # Debug info
                    start_snippet = "..."
                    if file_lines: start_snippet = file_lines[0][:50]
                    raise Exception(f"Context mismatch in {filename}. Search block not found (Fuzzy match failed).\nFile starts with: {start_snippet}")
            
            pending_writes[full_path] = "\n".join(file_lines)

        # Commit all changes
        for path, data in pending_writes.items():
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(data)
        
        return logs

# --- API ENDPOINTS (PRESERVED) ---

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
            
            # Save Manual Patch
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
        
        # 2. Create Backup (PRESERVED)
        backup_name = create_backup(root, current_v)
        if not backup_name:
            return jsonify({'success': False, 'message': 'Backup failed. Update aborted for safety.'}), 500

        # 3. Apply Patch (NEW ROBUST ENGINE)
        patcher = RobustPatcher()
        patch_content = patch_content.replace('\r\n', '\n')
        
        logs = patcher.apply(patch_content, root)
        
        # 4. Increment Version & Restart (PRESERVED)
        new_v = increment_version(root)
        
        log_text = f"BACKUP: {backup_name}\nVERSION: {current_v} -> {new_v}\n--------------------------\n" + "\n".join(logs)
        print(f"\n >>> PATCH SUCCESS:\n{log_text}\n")

        threading.Thread(target=restart_server).start()

        return jsonify({
            'success': True, 
            'message': f"System patched to v{new_v}. Restarting...",
            'logs': log_text
        })

    except Exception as e:
        print(f"PATCH ERROR: {str(e)}")
        return jsonify({'success': False, 'message': f"PATCH FAILED: {str(e)}"}), 500