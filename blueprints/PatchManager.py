import os
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user

patch_bp = Blueprint('patch_manager', __name__)

class SimplePatcher:
    """
    A robust, pure-Python patcher that ignores CRLF differences.
    """
    def apply(self, patch_content, root_path):
        lines = patch_content.splitlines()
        current_file = None
        hunks = []
        
        # 1. PARSE THE DIFF
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Detect File Target
            if line.startswith('+++ b/'):
                current_file = line[6:].strip()
                i += 1
                continue
            elif line.startswith('--- a/'):
                i += 1
                continue
                
            # Detect Hunk Header (@@ -x,y +x,y @@)
            if line.startswith('@@'):
                i += 1
                search_block = []
                replace_block = []
                
                # Parse Hunk Lines
                while i < len(lines):
                    h_line = lines[i]
                    
                    # STRICT HEADER CHECK (Fixes the "random +" artifact bug)
                    if h_line.startswith('diff ') or h_line.startswith('--- ') or h_line.startswith('+++ '):
                        break # End of hunk, start of next file
                    
                    # Logic:
                    # ' ' (space) = Context (keep in both)
                    # '-' (minus) = Delete (keep in search, skip in replace)
                    # '+' (plus)  = Add (skip in search, keep in replace)
                    
                    if h_line.startswith(' '):
                        content = h_line[1:]
                        search_block.append(content)
                        replace_block.append(content)
                    elif h_line.startswith('-'):
                        content = h_line[1:]
                        search_block.append(content)
                    elif h_line.startswith('+'):
                        content = h_line[1:]
                        replace_block.append(content)
                    
                    i += 1
                
                # Register Hunk
                if current_file:
                    hunks.append({
                        'file': current_file,
                        'search': "\n".join(search_block),
                        'replace': "\n".join(replace_block)
                    })
                continue
            
            i += 1

        # 2. APPLY HUNKS
        results = []
        for hunk in hunks:
            full_path = os.path.join(root_path, hunk['file'])
            
            if not os.path.exists(full_path):
                results.append(f"Skipped {hunk['file']} (Not found)")
                continue
                
            try:
                # Read file (Python handles CRLF normalization to \n)
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # SMART REVERSE: Check if the 'replace' block is already there
                if hunk['replace'] in content:
                    # Swap Replace -> Search (Undo)
                    new_content = content.replace(hunk['replace'], hunk['search'], 1)
                    
                    # Write back (Standard text mode preserves system line endings)
                    with open(full_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    results.append(f"Reverted patch in {hunk['file']}")
                    continue
                
                # NORMAL APPLY: Check if 'search' block is there
                if hunk['search'] in content:
                    new_content = content.replace(hunk['search'], hunk['replace'], 1)
                    with open(full_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    results.append(f"Patched {hunk['file']}")
                else:
                    results.append(f"Failed to match context in {hunk['file']}")
                    
            except Exception as e:
                results.append(f"Error patching {hunk['file']}: {str(e)}")
                
        return results

@patch_bp.route('/api/admin/patch/apply', methods=['POST'])
@login_required
def apply_patch():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file uploaded'}), 400

    file = request.files['file']
    if not file.filename.endswith('.diff'):
        return jsonify({'success': False, 'message': 'Invalid format. Only .diff files allowed.'}), 400

    try:
        # Read file content directly into string
        patch_content = file.read().decode('utf-8').replace('\r\n', '\n')
        
        patcher = SimplePatcher()
        logs = patcher.apply(patch_content, current_app.root_path)
        
        # Check success (Patched OR Reverted)
        success = any("Patched" in log or "Reverted" in log for log in logs)
        msg_text = "\n".join(logs)
        
        # LOG TO CMD CONSOLE (Requested Feature)
        print(f"\n >>> PATCH LOG:\n{msg_text}\n")

        if success:
            return jsonify({'success': True, 'message': "Update Applied.\n" + msg_text})
        else:
            return jsonify({'success': False, 'message': "Update Failed or No Changes Needed.\n" + msg_text})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500