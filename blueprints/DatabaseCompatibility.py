from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from sqlalchemy import text
from extensions import db
from utils import format_error

db_compat_bp = Blueprint('db_compat', __name__)

# --- NEW: GET CURRENT LEVEL ---
@db_compat_bp.route('/api/tools/db-compatibility/current', methods=['GET'])
@login_required
def get_current_compatibility():
    if current_user.role != 'admin': 
        return jsonify({'success': False, 'message': 'Unauthorized'})
    try:
        # Simple query to get the level of the current DB
        with db.engine.connect() as connection:
            result = connection.execute(text("SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()"))
            row = result.fetchone()
            current_level = row[0] if row else 'Unknown'
            
        return jsonify({'success': True, 'level': current_level})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# --- EXISTING: UPDATE LEVEL ---
@db_compat_bp.route('/api/tools/db-compatibility', methods=['POST'])
@login_required
def update_compatibility():
    if current_user.role != 'admin':
        return jsonify({'success': False, 'message': 'Unauthorized'})

    data = request.json
    target_level = data.get('target_level')
    is_confirmed = data.get('confirmed', False)

    if not target_level:
        return jsonify({'success': False, 'message': 'Target level required'})

    try:
        with db.engine.connect() as connection:
            connection = connection.execution_options(isolation_level="AUTOCOMMIT")

            # 1. Get Current Info
            result = connection.execute(text("SELECT name, compatibility_level FROM sys.databases WHERE name = DB_NAME()"))
            row = result.fetchone()
            
            if not row: raise Exception("Could not determine current database info")

            current_db = row[0]
            current_level = int(row[1])

            # 2. Validate
            try:
                new_level = int(target_level)
            except ValueError:
                return jsonify({'success': False, 'message': 'Level must be an integer'})

            if new_level < 130:
                return jsonify({'success': False, 'message': 'Security Block: Level cannot be lower than 130.'})

            if new_level < current_level and not is_confirmed:
                return jsonify({
                    'success': False, 
                    'requires_confirmation': True, 
                    'message': f"Warning: Downgrading from {current_level} to {new_level}. This may break features. Are you sure?"
                })

            # 3. Execute
            sql = f"ALTER DATABASE [{current_db}] SET COMPATIBILITY_LEVEL = {new_level}"
            connection.execute(text(sql))

        return jsonify({'success': True, 'message': f'Compatibility Level set to {new_level}'})

    except Exception as e:
        return jsonify({'success': False, 'message': format_error(e)})