import os
import sys
import time
import json
import zipfile
import threading
import logging
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

# --- PATCHER ENGINE ---

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
        """Applies the parsed patch to the filesystem."""
        lines = patch_content.splitlines()
        hunks = self.parse(lines)
        results = []
        
        # Normalize newlines in patch content
        patch_content = patch_content.replace('\r\n', '\n')
        
        if not hunks:
            raise Exception("No valid patch segments found. Check format.")

        for hunk in hunks:
            full_path = os.path.join(root_path, hunk['file'])
            
            # --- SCENARIO 1: FILE DOES NOT EXIST (Create It) ---
            if not os.path.exists(full_path):
                # If the 'search' block is empty (or we assume creation for missing files)
                # We interpret this as a File Creation request.
                try:
                    os.makedirs(os.path.dirname(full_path), exist_ok=True)
                    with open(full_path, 'w', encoding='utf-8') as f:
                        f.write(hunk['replace'])
                    results.append(f"Created: {hunk['file']}")
                    continue
                except Exception as e:
                    raise Exception(f"Failed to create file {hunk['file']}: {str(e)}")

            # --- SCENARIO 2: FILE EXISTS (Modify It) ---
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # Normalize line endings for comparison
                content_norm = content.replace('\r\n', '\n')
                search_norm = hunk['search'].replace('\r\n', '\n')
                
                if search_norm in content_norm:
                    # Perform replacement on normalized content
                    new_content = content_norm.replace(search_norm, hunk['replace'], 1)
                    
                    with open(full_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    results.append(f"Patched: {hunk['file']}")
                else:
                    # STRICT FAILURE: If we expected text but didn't find it, stop everything.
                    print(f"FAILED CONTEXT IN {hunk['file']}:\nExpected:\n{search_norm}\n\nFound start of file:\n{content_norm[:100]}")
                    raise Exception(f"Context mismatch in {hunk['file']}. The code to replace was not found.")
                    
            except Exception as e:
                # Re-raise to stop the entire process
                raise Exception(f"Critical Error processing {hunk['file']}: {str(e)}")
        
        return results

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