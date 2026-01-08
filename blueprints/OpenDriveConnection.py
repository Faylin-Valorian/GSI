import subprocess
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from utils import format_error  # <--- NEW IMPORT

open_drive_bp = Blueprint('open_drive', __name__)

@open_drive_bp.route('/api/tools/connect-drive', methods=['POST'])
@login_required
def connect_drive():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'})

    data = request.json
    drive = data.get('drive_letter')
    path = data.get('network_path')
    user = data.get('username')
    password = data.get('password')

    if not drive or not path:
        return jsonify({'success': False, 'message': 'Missing Drive Letter or Path'})

    # Basic Validation
    if len(drive) != 1 or not path.startswith('\\\\'):
        return jsonify({'success': False, 'message': 'Invalid format'})

    drive_letter = f"{drive}:"

    try:
        # 1. Disconnect if exists (Suppress errors)
        subprocess.run(f"net use {drive_letter} /delete /y", shell=True, stderr=subprocess.DEVNULL)

        # 2. Build Command
        cmd = f'net use {drive_letter} "{path}"'
        if password:
            cmd += f' "{password}"'
        if user:
            cmd += f' /user:"{user}"'

        # 3. Execute
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            return jsonify({'success': True, 'message': f'Successfully connected {drive_letter}'})
        else:
            # Raise error with the system output so format_error can decide what to show
            raise Exception(f"Command Failed: {result.stderr.strip() or result.stdout.strip()}")

    except Exception as e:
        # Use debug toggle helper
        msg = format_error(e)
        return jsonify({'success': False, 'message': msg})