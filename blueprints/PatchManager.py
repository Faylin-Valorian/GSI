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

# --- CORE UTILITIES (PRESERVED) ---

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

# --- NEW ANCHOR PATCHER ENGINE ---

class AnchorPatcher:
    def apply_anchors(self, root_path, patch_data):
        logs = []
        
        for file_rel_path, updates in patch_data.items():
            full_path = os.path.join(root_path, file_rel_path)
            
            # CASE 1: FULL FILE WRITE (If value is string)
            # Used for creating new files or overwriting small config files entirely
            if isinstance(updates, str):
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(updates)
                logs.append(f"Full Write: {file_rel_path}")
                continue

            # CASE 2: ANCHOR REPLACEMENT (If value is Dict of anchors)
            if not os.path.exists(full_path):
                logs.append(f"ERROR: File not found for anchoring: {file_rel_path}")
                continue

            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            file_modified = False
            
            for anchor_id, new_code in updates.items():
                # Regex looks for:
                # 1. Everything up to the start tag: [GSI_BLOCK: anchor_id]
                # 2. The middle content (lazy match)
                # 3. The end tag: [GSI_END: anchor_id]
                
                # We use re.DOTALL so (.) matches newlines
                pattern = re.compile(
                    r"(.*?\[GSI_BLOCK:\s*" + re.escape(anchor_id) + r"\].*?[\r\n]+)(.*?)([\r\n]+.*?\[GSI_END:\s*" + re.escape(anchor_id) + r"\].*?)",
                    re.DOTALL | re.IGNORECASE
                )
                
                if pattern.search(content):
                    # Replace the middle group (2) with new_code
                    content = pattern.sub(lambda m: f"{m.group(1)}{new_code}{m.group(3)}", content)
                    logs.append(f"Anchored: {file_rel_path} -> [{anchor_id}]")
                    file_modified = True
                else:
                    logs.append(f"WARNING: Anchor [{anchor_id}] not found in {file_rel_path}")

            if file_modified:
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(content)
        
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
        
        patch_json = None

        # 1. Get Patch JSON (File or Text)
        if 'file' in request.files:
            file = request.files['file']
            # Accept .json files
            if file.filename.endswith('.json'):
                try:
                    patch_json = json.load(file)
                except Exception as e:
                    return jsonify({'success': False, 'message': f'Invalid JSON file: {str(e)}'}), 400
            else:
                return jsonify({'success': False, 'message': 'Invalid file format. .json required.'}), 400

        elif 'patch_content' in request.form and request.form['patch_content'].strip():
            try:
                patch_json = json.loads(request.form['patch_content'])
            except Exception as e:
                return jsonify({'success': False, 'message': f'Invalid JSON text: {str(e)}'}), 400
        
        if not patch_json:
            return jsonify({'success': False, 'message': 'No patch data provided'}), 400

        root = current_app.root_path
        current_v = load_version(root)['string']
        
        # 2. Create Backup
        backup_name = create_backup(root, current_v)
        if not backup_name:
            return jsonify({'success': False, 'message': 'Backup failed. Update aborted.'}), 500

        # 3. Apply Anchors
        patcher = AnchorPatcher()
        logs = patcher.apply_anchors(root, patch_json)
        
        # 4. Increment Version
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